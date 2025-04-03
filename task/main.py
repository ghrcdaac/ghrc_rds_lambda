import datetime
import json
import os
import time
from abc import ABC
import traceback
from task.query_builders import build_query_case_1, build_query_case_2
from task.api_model import *

import boto3
import psycopg2
from psycopg2 import sql


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

def get_limit_sql(limit):
    limit_sql = sql.SQL('')
    if limit >= 0:
        limit_sql = sql.SQL(
            """
            LIMIT {}
            """
        ).format(sql.SQL(str(limit)))

    return limit_sql

def get_async_join(columns, where, right_table, limit):
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
    sql_where = sql.SQL('')
    if where:
        sql_where = sql.SQL('WHERE {}').format(sql.SQL(where))

    return sql_where

def build_granules_query(records, columns, where='', limit=-1):
    joins = []
    for get_join in [get_collection_id_join, get_executions_join, get_files_array_join, get_providers_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins


def build_rules_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_collection_json_join, get_providers_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def build_executions_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_async_join, get_collection_id_join, get_executions_join]:
       joins.append(get_join(columns, where, records))

    joins = sql.SQL(' ').join(joins)

    return joins

def build_pdrs_query(records, columns=None, where=None, limit=-1):
    joins = []
    for get_join in [get_async_join, get_collection_id_join, get_executions_join]:
       joins.append(get_join(columns, where, records, limit))

    joins = sql.SQL(' ').join(joins)

    return joins

def get_empty_sql_object(records, columns=None, where=None, limit=-1):
    return sql.SQL('')

def build_query_new(records, columns=None, where=None, limit=0, **rds_config):
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


def main(event, context):
    handler_args = {}
    print_query = ''
    try:
        rds_config = event.get('rds_config')
        print(rds_config)
        handler_args = {
            'bucket': os.getenv('BUCKET_NAME'),
            'key': f'{os.getenv("S3_KEY_PREFIX")}query_results_{time.time_ns()}.json'
        }

        client = boto3.client('s3')
        client.put_object(Bucket=handler_args['bucket'], Key=handler_args['key'], Body=b'[]')
        query = temp_query_selection(**rds_config)
        with psycopg2.connect(**get_db_params()) as db_conn:
            with db_conn.cursor(name='rds-cursor') as curs:
                curs.itersize = event.get('size', 10000)

                print_query = '\r'.join(query.as_string(curs).replace('\n', '\r').split('\r'))
                # print(print_query)  # Uncomment when troubleshooting queries
                # print(curs.mogrify(query, vars))
                curs.execute(query=query)

                upload_handler = MPUHandler(**handler_args)
                rowcount = 0
                for row in curs:
                    upload_handler.handle_row(row, curs.description)
                    rowcount += 1

                upload_handler.complete_upload()

                handler_args.update({
                    'query': print_query,
                    'count': rowcount,
                    'records': rds_config.get('records')
                })
    except Exception as e:
        print(e)
        stack_trace = traceback.format_exc()
        handler_args.update({'exception': repr(e), 'stack_trace': stack_trace})

    print(handler_args)
    return handler_args


if __name__ == '__main__':
    pass
