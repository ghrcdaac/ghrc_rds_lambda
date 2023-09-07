import datetime
import json
import math
import os
import re
import time
from abc import ABC

import boto3
import psycopg2
from psycopg2 import sql

CUMULUS_DB_COLUMNS = {
    'cumulus_id', 'granule_id', 'status', 'collection_cumulus_id', 'created_at', 'updated_at', 'published', 'duration',
    'time_to_archive', 'time_to_process', 'product_volume', 'error', 'cmr_link', 'pdr_cumulus_id',
    'provider_cumulus_id', 'beginning_date_time', 'ending_date_time', 'last_update_date_time',
    'processing_end_date_time', 'processing_start_date_time', 'production_date_time', 'query_fields', 'timestamp',
    'cumulus_id', 'name', 'version', 'sample_file_name', 'granule_id_validation_regex', 'granule_id_extraction_regex',
    'files', 'process', 'url_path', 'duplicate_handling', 'report_to_ems', 'ignore_files_config_for_discovery', 'meta',
    'tags', 'created_at', 'updated_at'
}


def build_query(records, where=None, columns=None, limit=100, **kwargs):
    keywords = {'and', 'or', 'not', 'in', 'like'}

    query_args = []
    if where:
        res = re.findall(r'[\w+\%]+', where)
        for word in res:
            lower_word = word.lower()
            if lower_word not in CUMULUS_DB_COLUMNS and lower_word not in keywords:
                where = where.replace(word, '%s', 1)
                query_args.append(word)
        sql_where = sql.SQL('WHERE {}').format(sql.SQL(where))
    else:
        sql_where = sql.SQL('')

    # TODO: Remove once we are sure the user input doesn't need to be escaped when a literal % is present.
    #  As of 08/31/2023 no issue has been discovered
    # if '%' in where:
    #     where = where + ' ESCAPE \'\''

    query = sql.SQL(
        'SELECT {} FROM {} '
        'JOIN collections ON granules.collection_cumulus_id=collections.cumulus_id '
        '{} '
        '{}'
    ).format(
        sql.SQL(', ').join([sql.Identifier(column) for column in columns]) if columns else sql.SQL('*'),
        sql.Identifier(records),
        sql_where,
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit else sql.SQL('')
    )

    return {'query': query, 'args': query_args}


def get_db_params():
    sm = boto3.client('secretsmanager')
    secrets_arn = os.getenv('CUMULUS_CREDENTIALS_ARN', None)
    db_init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))
    db_init_kwargs.update({'user': db_init_kwargs.pop('username')})

    return db_init_kwargs


class UploadHandlerBase(ABC):
    def handle_row(self, row, selected_columns):
        raise NotImplementedError

    def complete_upload(self):
        raise NotImplementedError

    @staticmethod
    def convert_tuple_to_json(row, selected_columns):
        record_dict = {}
        for value, index in zip(row, range(len(row))):
            if isinstance(value, datetime.datetime):
                value = str(value)
            elif isinstance(value, bool):
                value = json.dumps(value)
            record_dict.update({selected_columns[index]: value})
        return json.dumps(record_dict)


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

    def handle_row(self, row, selected_columns):
        self.rows.append(self.convert_tuple_to_json(row, selected_columns))
        column_count = len(self.rows) * len(selected_columns)
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


class UploadHandler(UploadHandlerBase):
    def __init__(self, bucket, key):
        self.s3_dict = {'Bucket': bucket, 'Key': key}
        self.rows = []

    def handle_row(self, row, selected_columns):
        self.rows.append(self.convert_tuple_to_json(row, selected_columns))

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


def main(event, context):
    rds_config = event.get('rds_config')
    query_dict = build_query(**rds_config)
    db_conn = psycopg2.connect(**get_db_params())
    curs = None
    try:
        db_conn.set_session(readonly=True)
        db_conn.commit()
        curs = db_conn.cursor()

        # print(query_dict.get('query').as_string(curs))  # Uncomment when troubleshooting queries
        curs.execute(query_dict.get('query'), query_dict.get('args'))

        handler_args = {
            'bucket': os.getenv('BUCKET_NAME'),
            'key': f'{os.getenv("S3_KEY_PREFIX")}query_results_{time.time_ns()}.json'
        }

        selected_columns = ([desc[0] for desc in curs.description])
        upload_handler = get_upload_handler(len(selected_columns) * curs.rowcount, handler_args)
        handler_args.update({'count': curs.rowcount})

        size = event.get('size', 10000)
        for _ in range(math.ceil(curs.rowcount / size)):
            for row in curs.fetchmany(size=size):
                upload_handler.handle_row(row, selected_columns)

        upload_handler.complete_upload()
        curs.close()

    finally:
        if curs and not curs.closed:
            curs.close()
        db_conn.close()

    return handler_args


if __name__ == '__main__':
    pass
