import datetime
import json
import os
import time
from abc import ABC
from task.api_model import *

import boto3
import psycopg2
from psycopg2 import sql

# CUMULUS_DB_COLUMNS = (
#     'cumulus_id', 'granule_id', 'status', 'collection_cumulus_id', 'created_at', 'updated_at', 'published', 'duration',
#     'time_to_archive', 'time_to_process', 'product_volume', 'error', 'cmr_link', 'pdr_cumulus_id',
#     'provider_cumulus_id', 'beginning_date_time', 'ending_date_time', 'last_update_date_time',
#     'processing_end_date_time', 'processing_start_date_time', 'production_date_time', 'query_fields', 'timestamp',
#     'cumulus_id', 'name', 'version', 'sample_file_name', 'granule_id_validation_regex', 'granule_id_extraction_regex',
#     'files', 'process', 'url_path', 'duplicate_handling', 'report_to_ems', 'ignore_files_config_for_discovery', 'meta',
#     'tags', 'created_at', 'updated_at', 'collection_id'
# )

# CUMULUS_DB_TABLES = (
#     'granules', 'collections', 'rules', 'files', 'granules_executions', 'executions', 'async_operations', 'providers', 'pdrs'
# )

def get_db_params():
    sm = boto3.client('secretsmanager')
    secrets_arn = os.getenv('CUMULUS_CREDENTIALS_ARN', None)
    secrets = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))

    db_params = {'sslmode': 'disable'} # Will revisit when/if SSL becomes required
    for key in secrets.keys():
        if key in ('username', 'user', 'password', 'database', 'host', 'port'):
            new_key = key
            if key == 'username':
                new_key = 'user'
            db_params.update({new_key: secrets.get(key)})

    return db_params


class UploadHandlerBase(ABC):
    def handle_row(self, row, selected_columns):
        raise NotImplementedError

    def complete_upload(self):
        raise NotImplementedError


class MPUHandler(UploadHandlerBase):
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
        if not self.s3_parts:
            body_string = f'[{body_string}'
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
        return self.s3_client.complete_multipart_upload(**complete_mpu_dict)

def convert_tuple_to_json(row, selected_columns):
        # print(f'selected_columns: {selected_columns}')
        record_dict = {}
        for value, index in zip(row, range(len(row))):
            if isinstance(value, datetime.datetime):
                value = str(value)
            elif isinstance(value, bool):
                value = json.dumps(value)

            # print(f'selected_columns[index]: {selected_columns[index]}')
            record_dict.update({selected_columns[index].name: value})
        return json.dumps(record_dict)


class UploadHandler(UploadHandlerBase):
    def __init__(self, bucket, key):
        self.s3_dict = {'Bucket': bucket, 'Key': key}
        self.rows = []

    def handle_row(self, row, selected_columns):
        self.rows.append(convert_tuple_to_json(row, selected_columns))

    def complete_upload(self):
        s3_client = boto3.client('s3')
        self.s3_dict.update({'Body': f'[{",".join(self.rows)}]'.encode()})
        return s3_client.put_object(**self.s3_dict)


def get_upload_handler(total_columns, handler_args):
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
    ret = False
    if selected_columns == '*' or any([column in table_columns for column in selected_columns.replace(' ', '').split(',')]):
        ret = True
    elif any([column in where for column in table_columns]):
        ret = True
    return ret

def get_async_join(columns, where, right_table):
    collections_join = sql.SQL('')
    if join_check(columns, where, async_operations_db_columns):
        collections_join = sql.SQL(
            '''
            LEFT JOIN (
                SELECT cumulus_id, id
                FROM async_operations
            ) AS async_operations ON async_operations.cumulus_id={}
            '''
            ).format(sql.Identifier(right_table, 'async_operation_cumulus_id'))

    return collections_join

def get_collection_json_join(columns, where, right_table):
    collections_join = sql.SQL('')
    if join_check(columns, where, collections_db_columns):
        collections_join = sql.SQL(
            '''
            JOIN (
            SELECT cumulus_id, json_build_object('collection', json_build_object('name', name, 'version', version))
            FROM collections
            ) AS GC ON GC.cumulus_id={}
            '''
            ).format(sql.Identifier(right_table, 'collection_cumulus_id'))

    return collections_join



def get_collection_id_join(columns, where, right_table):
    collections_join = sql.SQL('')
    if join_check(columns, where, collections_db_columns):
        collections_join = sql.SQL(
            '''
            JOIN (
                SELECT cumulus_id, concat(collections.name, '___', collections.version) AS collection_id
                FROM collections
            ) AS GC ON GC.cumulus_id={}
            '''
            ).format(sql.Identifier(right_table, 'collection_cumulus_id'))

    return collections_join

def get_executions_join(columns, where, right_table):
    executions_join = sql.SQL('')
    if join_check(columns, where, executions_db_columns):
        executions_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT DISTINCT ON (granule_cumulus_id) granule_cumulus_id, url AS execution
          FROM executions
          JOIN granules_executions ON executions.cumulus_id=granules_executions.execution_cumulus_id
          ORDER BY granule_cumulus_id, executions.timestamp
        ) AS execution_arns ON execution_arns.granule_cumulus_id={}
        '''
    ).format(sql.Identifier(right_table, 'cumulus_id'))

    return executions_join

def get_files_array_join(columns, where, right_table):
    files_join = sql.SQL('')
    if join_check(columns, where, files_db_columns):
        files_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT granule_cumulus_id, json_agg(files) AS files
          FROM files
          GROUP BY granule_cumulus_id
        ) AS granule_files on granule_files.granule_cumulus_id={}
        '''
    ).format(sql.Identifier(right_table, 'cumulus_id'))

    return files_join

def get_providers_join(columns, where, right_table):
    providers_join = sql.SQL('')
    if join_check(columns, where, providers_db_columns):
        providers_join = sql.SQL(
        '''
        LEFT JOIN (
          SELECT name AS provider, providers.cumulus_id
          FROM providers
        ) AS provider_names ON provider_names.cumulus_id={}
        '''
    ).format(sql.Identifier(right_table, 'provider_cumulus_id'))
    
    return providers_join

def build_where(where=''):
    sql_where = sql.SQL('')
    if where:
        sql_where = sql.SQL('WHERE {}').format(sql.SQL(where))

    return sql_where

def build_granules_query(records, columns, where='', limit=-1):
    joins = []
    for get_join in [get_collection_id_join, get_executions_join, get_files_array_join, get_providers_join]:
       joins.append(get_join(columns, where, records))

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
        sql.SQL(' ').join(joins),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query


def build_rules_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_collection_json_join, get_providers_join]:
       joins.append(get_join(columns, where, records))

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
        sql.SQL(' ').join(joins),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_collections_query(records, columns=None, where=None, limit=-1):
    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        '''
    ).format(
        sql.SQL(columns if columns else '*'),
        sql.Identifier(records),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_executions_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_async_join, get_collection_id_join, get_executions_join]:
       joins.append(get_join(columns, where, records))

    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        {}
        '''
    ).format(
        # sql.SQL(', ').join([sql.Identifier(column) for column in columns]) if columns else sql.SQL('*'),
        sql.SQL(columns if columns else '*'),
        sql.Identifier(records),
        sql.SQL(' ').join(joins),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_providers_query(table, columns=None, where=None, limit=-1):
    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        '''
    ).format(
        # sql.SQL(', ').join([sql.Identifier(column) for column in columns]) if columns else sql.SQL('*'),
        sql.SQL(columns if columns else '*'),
        sql.Identifier(table),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_pdrs_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_collection_id_join, get_providers_join, get_executions_join]:
       joins.append(get_join(columns, where, records))

    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        {}
        '''
    ).format(
        # sql.SQL(', ').join([sql.Identifier(column) for column in columns]) if columns else sql.SQL('*'),
        sql.SQL(columns if columns else '*'),
        sql.Identifier(records),
        sql.SQL(' ').join(joins),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_async_query(table, columns=None, where=None, limit=-1):
    query = sql.SQL(
        '''
        SELECT {}
        FROM {}
        {}
        {}
        '''
    ).format(
        # sql.SQL(', ').join([sql.Identifier(column) for column in columns]) if columns else sql.SQL('*'),
        sql.SQL(columns if columns else '*'),
        sql.Identifier(table),
        build_where(where),
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit >= 0 else sql.SQL('')
    )

    return query

def build_query_new(records, columns=None, where=None, limit=0):
    if not columns:
        columns = '*'

    switch = {
        'granules': build_granules_query,
        'rules': build_rules_query,
        'collections': build_collections_query,
        'executions': build_executions_query,
        'providers': build_providers_query,
        'pdrs': build_pdrs_query,
        'async_operations': build_async_query
    }

    return switch.get(records)(records, columns, where, limit)

def main(event, context):
    rds_config = event.get('rds_config')
    handler_args = {
        'bucket': os.getenv('BUCKET_NAME'),
        'key': f'{os.getenv("S3_KEY_PREFIX")}query_results_{time.time_ns()}.json'
    }

    client = boto3.client('s3')
    client.put_object(Bucket=handler_args['bucket'], Key=handler_args['key'], Body=b'[]')

    query = build_query_new(**rds_config)
    with psycopg2.connect(**get_db_params()) as db_conn:
        with db_conn.cursor(name='rds-cursor') as curs:
            curs.itersize = event.get('size', 10000)
            print(query.as_string(curs))  # Uncomment when troubleshooting queries
            # print(curs.mogrify(query, vars))
            curs.execute(query=query)

            upload_handler = MPUHandler(**handler_args)
            rowcount = 0
            for row in curs:
                upload_handler.handle_row(row, curs.description)
                rowcount += 1
            handler_args.update({'count': rowcount})

            upload_handler.complete_upload()

    return handler_args


if __name__ == '__main__':
    pass
