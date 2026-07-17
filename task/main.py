import datetime
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from abc import ABC
import traceback
from task.query_builders import build_query_case_1, build_query_case_2
from task.api_model import *

import boto3
import psycopg2
from psycopg2 import sql


DEFAULT_OUTPUT_MODE = "multipart_s3"

VALID_OUTPUT_MODES = {
    "multipart_s3",
    "single_s3",
    "s3_batches",
}


@dataclass(frozen=True)
class OutputConfig:
    """Dataclass for output configuration."""
    mode: str = DEFAULT_OUTPUT_MODE
    batch_size: int = 10_000


def parse_output_config(event: dict) -> OutputConfig:
    """Parse output config from event.

    Arguments:
        event: dict[str, Any]
            Output config from event.

    Returns:
        OutputConfig
    """
    raw_config = event.get("output") or {}

    mode = raw_config.get("mode", DEFAULT_OUTPUT_MODE)

    if mode not in VALID_OUTPUT_MODES:
        valid_modes = ", ".join(sorted(VALID_OUTPUT_MODES))
        raise ValueError(
            f'Unsupported output mode "{mode}". '
            f"Valid modes: {valid_modes}"
        )

    batch_size = int(raw_config.get("batch_size", 10_000))

    if batch_size <= 0:
        raise ValueError("output.batch_size must be greater than zero")

    return OutputConfig(
        mode=mode,
        batch_size=batch_size,
    )


def get_db_params():
    """Get parameters for AWS database."""
    sm = boto3.client(service_name='secretsmanager', region_name='us-west-2')
    secrets_arn = os.getenv('CUMULUS_CREDENTIALS_ARN', None)
    secrets = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))

    query_timeout_offset = 1000
    statement_timeout_ms = int(os.getenv("QUERY_TIMEOUT")) * 1000 - query_timeout_offset
    db_params = {
        'sslmode': 'disable', # Will revisit when/if SSL becomes required
        'options': f'-c statement_timeout={statement_timeout_ms}'
    }
    for key in secrets.keys():
        if key in ('username', 'user', 'password', 'database', 'host', 'port'):
            new_key = key
            if key == 'username':
                new_key = 'user'
            db_params.update({new_key: secrets.get(key)})

    return db_params

def convert_tuple_to_json(row, selected_columns):
    """Convert tuple to JSON."""
    # print(f'selected_columns: {selected_columns}')
    record_dict = {}
    for value, index in zip(row, range(len(row))):
        if isinstance(value, datetime.datetime):
            value = str(value)

        # print(f'selected_columns[index]: {selected_columns[index]}')
        record_dict.update({selected_columns[index].name: value})
    return json.dumps(record_dict)


#----------------------------------------------------------------------
# Upload/output handlers
#----------------------------------------------------------------------

class UploadHandlerBase(ABC):
    """Base class for UploadHandlers."""
    def handle_row(self, row, selected_columns):
        """Handle output row."""
        raise NotImplementedError

    def complete_upload(self):
        """Package output and complete upload."""
        raise NotImplementedError


class MPUHandler(UploadHandlerBase):
    """UploadHandler for multipart S3 uploads."""
    def __init__(self, bucket, key):
        self.s3_parts = []
        self.s3_client = boto3.client('s3')
        s3_dict = {'Bucket': bucket, 'Key': key}
        mpu_dict = self.s3_client.create_multipart_upload(**s3_dict)
        s3_dict.update({'UploadId': mpu_dict.get('UploadId')})
        self.s3_mpu_dict = s3_dict
        self.s3_part_size = 20971520  # 20MBit
        self.rows = []

    def handle_row(self, row, column_description):
        self.rows.append(convert_tuple_to_json(row, column_description))
        column_count = len(self.rows) * len(column_description)
        if column_count >= 360000:
            self.upload_part(f'{",".join(self.rows)}')
            self.rows.clear()

    def upload_part(self, body_string):
        """Upload one part of multipart upload."""
        if not self.s3_parts:
            body_string = f'[{body_string}'
        else:
            body_string = f', {body_string}'
        part_number_dict = {'PartNumber': len(self.s3_parts) + 1}
        mpu_upload_dict = {**part_number_dict, **self.s3_mpu_dict}
        mpu_upload_dict.update({'Body': body_string.encode()})
        rsp = self.s3_client.upload_part(**mpu_upload_dict)
        part_number_dict.update({'ETag': rsp.get('ETag')})
        self.s3_parts.append(part_number_dict)

    def complete_upload(self):
        remaining_rows = f'{",".join(self.rows)}]'
        self.upload_part(remaining_rows)
        complete_mpu_dict = {
            **self.s3_mpu_dict, **{
                'MultipartUpload': {
                    'Parts': self.s3_parts
                }
            }
        }

        s3_response = self.s3_client.complete_multipart_upload(
            **complete_mpu_dict
        )

        return {
            "output_mode": "multipart_s3",
            "bucket": self.s3_mpu_dict["Bucket"],
            "key": self.s3_mpu_dict["Key"],
            "etag": s3_response.get("ETag"),
        }


class UploadHandler(UploadHandlerBase):
    """UploadHandler for single S3 upload."""
    def __init__(self, bucket, key):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        self.s3_dict = {'Bucket': bucket, 'Key': key}
        self.rows = []

    def handle_row(self, row, selected_columns):
        self.rows.append(convert_tuple_to_json(row, selected_columns))

    def complete_upload(self):
        body = f'[{",".join(self.rows)}]'.encode("utf-8")

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=body,
            ContentType="application/json",
        )

        return {
            "output_mode": "single_s3",
            "bucket": self.bucket,
            "key": self.key,
        }


class S3BatchHandler(UploadHandlerBase):
    """UploadHandler for batched S3 uploads."""
    def __init__(
        self,
        bucket: str,
        prefix: str,
        batch_size: int,
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.batch_size = batch_size

        self.s3_client = boto3.client("s3")
        self.rows = []
        self.total_rows = 0
        self.batches = []

    def handle_row(self, row, selected_columns):
        serialized_row = convert_tuple_to_json(
            row,
            selected_columns,
        )

        self.rows.append(serialized_row)
        self.total_rows += 1

        if len(self.rows) >= self.batch_size:
            self._flush_batch()

    def _flush_batch(self):
        if not self.rows:
            return

        batch_number = len(self.batches) + 1
        batch_key = (
            f"{self.prefix}/"
            f"batch-{batch_number:06d}.json"
        )

        body = f'[{",".join(self.rows)}]'.encode("utf-8")

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=batch_key,
            Body=body,
            ContentType="application/json",
        )

        self.batches.append({
            "number": batch_number,
            "key": batch_key,
            "row_count": len(self.rows),
            "byte_count": len(body),
        })

        self.rows.clear()

    def complete_upload(self):
        # Publish any final partial batch.
        self._flush_batch()

        manifest_key = f"{self.prefix}/manifest.json"

        manifest = {
            "status": "complete",
            "total_rows": self.total_rows,
            "batch_count": len(self.batches),
            "batches": self.batches,
        }

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=manifest_key,
            Body=json.dumps(manifest).encode("utf-8"),
            ContentType="application/json",
        )

        return {
            "output_mode": "s3_batches",
            "bucket": self.bucket,
            "prefix": f"{self.prefix}/",
            "manifest_key": manifest_key,
            "batch_count": len(self.batches),
        }


#----------------------------------------------------------------------

def create_upload_handler(
    config: OutputConfig,
    bucket: str,
    key: str,
) -> UploadHandlerBase:
    """
    Create appropriate upload handler for request.

    Parameters:
        config: OutputConfig
            OutputConfig object from parse_output_config, derived from event.
        bucket: str
            S3 bucket name.
        key: str
            S3 object key.

    Returns:
        Derived UploadHandler object.
    """

    if config.mode == "multipart_s3":
        return MPUHandler(
            bucket=bucket,
            key=key,
        )

    if config.mode == "single_s3":
        return UploadHandler(
            bucket=bucket,
            key=key,
        )

    if config.mode == "s3_batches":
        batch_prefix = key.removesuffix(".json")

        return S3BatchHandler(
            bucket=bucket,
            prefix=batch_prefix,
            batch_size=config.batch_size,
        )

    # parse_output_config() should prevent this.
    raise ValueError(f"Unsupported output mode: {config.mode}")

def get_upload_handler(total_columns, handler_args):
    """
    Get an upload handler based on estimated request size.

    Generally, we should now use explicit handler selection with  create_upload_handler.

    Parameters:
        total_columns: int
            Number of requested columns
        handler_args: dict[str, Any]
            Arguments for handler
    Returns:
        Upload handler
    """
    size_avg = 70  # 70 bytes
    bytes_estimate = total_columns * size_avg
    if bytes_estimate >= 52428800:  # 50MBit
        upload_handler = MPUHandler(**handler_args)
        print('multipart upload')
    else:
        print('single upload')
        upload_handler = UploadHandler(**handler_args)

    return upload_handler

def join_check(selected_columns, where, table_columns):
    """Return whether the selected columns or WHERE clause require a table join."""
    ret = False
    if selected_columns == '*' or any([column in table_columns for column in selected_columns.replace(' ', '').split(',')]):
        ret = True
    elif any([column in where for column in table_columns]):
        ret = True
    return ret

def get_limit_sql(limit):
    """Build a SQL LIMIT clause for a nonnegative limit."""
    limit_sql = sql.SQL('')
    if limit >= 0:
        limit_sql = sql.SQL(
            """
            LIMIT {}
            """
        ).format(sql.SQL(str(limit)))

    return limit_sql

def get_async_join(columns, where, right_table, limit):
    """Build a SQL LIMIT clause for a nonnegative limit."""
    collections_join = sql.SQL('')
    if join_check(columns, where, async_operations_db_columns):
        collections_join = sql.SQL(
            '''
            LEFT JOIN (
                SELECT cumulus_id, id AS async_operation_id
                FROM async_operations
                {}
            ) AS async_operations ON async_operations.cumulus_id={}
            '''
            ).format(get_limit_sql(limit), sql.Identifier(right_table, 'async_operation_cumulus_id'))

    return collections_join

def get_collection_json_join(columns, where, right_table, limit):
    """Build an optional join that returns collection data as JSON."""
    collections_join = sql.SQL('')
    if join_check(columns, where, collections_db_columns):
        collections_join = sql.SQL(
            '''
            JOIN (
            SELECT cumulus_id, json_build_object('collection', json_build_object('name', name, 'version', version))
            FROM collections
            {}
            ) AS GC ON GC.cumulus_id={}
            '''
            ).format(get_limit_sql(limit), sql.Identifier(right_table, 'collection_cumulus_id'))

    return collections_join

def get_collection_id_join(columns, where, right_table, limit):
    """Build an optional join that returns a combined collection identifier."""
    collections_join = sql.SQL('')
    if join_check(columns, where, collections_db_columns):
        collections_join = sql.SQL(
            '''
            JOIN (
                SELECT cumulus_id, concat(collections.name, '___', collections.version) AS collection_id
                FROM collections
                {}
            ) AS GC ON GC.cumulus_id={}
            '''
            ).format(get_limit_sql(limit), sql.Identifier(right_table, 'collection_cumulus_id'))

    return collections_join

def get_executions_join(columns, where, right_table, limit):
    """Build an optional join that returns one execution URL per granule."""
    executions_join = sql.SQL('')
    if join_check(columns, where, executions_db_columns):
        executions_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT DISTINCT ON (granule_cumulus_id) granule_cumulus_id, url AS execution
          FROM executions
          JOIN granules_executions ON executions.cumulus_id=granules_executions.execution_cumulus_id
          ORDER BY granule_cumulus_id, executions.timestamp
          {}
        ) AS execution_arns ON execution_arns.granule_cumulus_id={}
        '''
    ).format(get_limit_sql(limit), sql.Identifier(right_table, 'cumulus_id'))

    return executions_join

def get_files_array_join(columns, where, right_table, limit):
    """Build an optional join that aggregates files for each granule."""
    files_join = sql.SQL('')
    if join_check(columns, where, files_db_columns):
        files_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT granule_cumulus_id, json_agg(files) AS files
          FROM files
          GROUP BY granule_cumulus_id
          {}
        ) AS granule_files on granule_files.granule_cumulus_id={}
        '''
    ).format(get_limit_sql(limit), sql.Identifier(right_table, 'cumulus_id'))

    return files_join

def get_providers_join(columns, where, right_table, limit):
    """Build an optional join that returns the provider name."""
    providers_join = sql.SQL('')
    if join_check(columns, where, providers_db_columns):
        providers_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT name AS provider, providers.cumulus_id
          FROM providers
          {}
        ) AS provider_names ON provider_names.cumulus_id={}
        '''
    ).format(get_limit_sql(limit), sql.Identifier(right_table, 'provider_cumulus_id'))
    
    return providers_join

def build_where(where=''):
    """Build a SQL WHERE clause when a condition is provided."""
    sql_where = sql.SQL('')
    if where:
        sql_where = sql.SQL('WHERE {}').format(sql.SQL(where))

    return sql_where

def build_granules_query(records, columns, where='', limit=-1):
    """Build the joins required for a granules query."""
    joins = []
    for get_join in [get_collection_id_join, get_executions_join, get_files_array_join, get_providers_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def build_rules_query(records, columns=None, where=None, limit=-1):
    """Build the joins required for a rules query."""
    joins = []
    for get_join in [get_collection_json_join, get_providers_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def build_executions_query(records, columns=None, where=None, limit=-1):
    """Build the joins required for an executions query."""
    joins = []
    for get_join in [get_async_join, get_collection_id_join, get_executions_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def build_pdrs_query(records, columns=None, where=None, limit=-1):
    """Build the joins required for a PDRs query."""
    joins = []
    for get_join in [get_async_join, get_collection_id_join, get_executions_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def get_empty_sql_object(records, columns=None, where=None, limit=-1):
    """Return an empty SQL fragment when no joins are required."""
    return sql.SQL('')

def build_query_new(records, columns=None, where=None, limit=0, **rds_config):
    """Build a SELECT query with its required joins, filters, and limit."""
    if not columns:
        columns = '*'

    joins_switch = {
        'granules': build_granules_query,
        'rules': build_rules_query,
        'executions': build_executions_query,
        'pdrs': build_pdrs_query
    }

    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        {}
        '''
    ).format(
        sql.SQL(columns if columns else '*'),
        sql.Identifier(records),
        joins_switch.get(records, get_empty_sql_object)(records, columns, where, limit),
        build_where(where),
        get_limit_sql(limit)
    )

    return query

def temp_query_selection(records, **rds_config):
    """Select the appropriate query builder for the requested record type."""
    query = ''
    if records == 'granules':
        if any(where in rds_config for where in ['granules_where', 'collections_where', 'providers_where', 'pdrs_where']):
            print('CASE 1')
            query = build_query_case_1(**rds_config)
        else:
            print('CASE 2')
            query = build_query_case_2(**rds_config)
        query = sql.SQL(query)
    else:
        query = build_query_new(records, **rds_config)

    return query

def main(event: dict[str, Any], _context) -> dict[str, Any]:
    """
    Main lambda procedure.

    Parameters:
        event (dict): Lambda invocation payload containing the RDS query
            configuration and output settings.
        _context: AWS Lambda runtime context object. Provides invocation
            metadata and remaining execution time. Currently unused.

    Returns:
        dict: Metadata describing the generated S3 output.
    """

    handler_args = {}
    print_query = ''
    db_conn = None

    try:
        rds_config = event.get('rds_config')
        output_config = event.get('output')
        print(rds_config)
        bucket = os.getenv("BUCKET_NAME")
        key = (
            f'{os.getenv("S3_KEY_PREFIX")}'
            f"query_results_{time.time_ns()}.json"
        )

        # `output` will support:
        #   multipart_s3   One final S3 object
        #   single_s3      Buffer everything; one normal PutObject
        #   s3_batches     Several independently readable S3 objects
        #       batch_size     Number of records in batch

        output_config = parse_output_config(event)
        query = temp_query_selection(**rds_config)

        db_conn = psycopg2.connect(**get_db_params())

        with db_conn.cursor(name="rds-cursor") as curs:
            curs.itersize = event.get("size", 10_000)

            print_query = query.as_string(curs)
            curs.execute(query=query)

            upload_handler = create_upload_handler(
                config=output_config,
                bucket=bucket,
                key=key,
            )

            rowcount = 0

            for row in curs:
                upload_handler.handle_row(
                    row,
                    curs.description,
                )
                rowcount += 1

            output_result = upload_handler.complete_upload()

            handler_args.update(output_result)

            handler_args.update({
                "query": print_query,
                "count": rowcount,
                "records": rds_config.get("records"),
            })
    except Exception as e:
        print(e)
        stack_trace = traceback.format_exc()
        handler_args.update({'exception': repr(e), 'stack_trace': stack_trace})
    finally:
        if db_conn is not None:
            db_conn.close()

    print(handler_args)
    return handler_args


if __name__ == '__main__':
    pass
