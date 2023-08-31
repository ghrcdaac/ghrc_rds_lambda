import datetime
import json
import os
import re
import time

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


def build_query(records, where=None, columns=None, limit=100):
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

    # TODO: Remove once we are sure the user input doesn't need to be escaped. So far no issue has been discovered
    # if '%' in where:
    #     where = where + ' ESCAPE \'\''

    query = sql.SQL(
        'SELECT {} FROM {} '
        'JOIN collections ON granules.collection_cumulus_id=collections.cumulus_id '
        '{} '
        '{}'
    ).format(
        sql.SQL(', ').join([sql.Identifier(x) for x in columns]) if columns else sql.SQL('*'),
        sql.Identifier(records),
        sql_where,
        sql.SQL('LIMIT {}').format(sql.SQL(str(limit))) if limit else sql.SQL('')
    )

    return {'query': query, 'args': query_args}


def main(event, context):
    sm = boto3.client('secretsmanager')
    secrets_arn = os.getenv('cumulus_credentials_arn', None)
    db_init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))
    db_init_kwargs.update({'user': db_init_kwargs.pop('username')})

    rds_config = event.get('rds_config')

    query_dict = build_query(**rds_config)
    db_conn = psycopg2.connect(**db_init_kwargs)
    curs = None
    try:
        db_conn.set_session(readonly=True)
        db_conn.commit()
        curs = db_conn.cursor()

        print(query_dict.get('query').as_string(curs))
        qes = time.time()
        curs.execute(query_dict.get('query'), query_dict.get('args'))
        print(f'execute: {time.time() - qes}')
        selected_columns = ([desc[0] for desc in curs.description])
        # s3_client = boto3.client('s3')

        # TODO: Implement multipart uploads for query results
        # mpu_dict = {
        #     'Bucket': '',
        #     'Key': ''
        # }
        # rsp = s3_client.create_multipart_upload(**mpu_dict)
        # mpu_dict.update({'UploadId': rsp.get('UploadId')})
        results = []
        while True:
            qfs = time.time()
            query_results = curs.fetchmany(size=1000)
            print(f'fetch: {time.time() - qfs}')

            if not query_results:
                break

            qis = time.time()
            for res in query_results:
                record_dict = {}
                for value, index in zip(res, range(len(res))):
                    if isinstance(value, datetime.datetime):
                        value = str(value)
                    elif isinstance(value, bool):
                        value = json.dumps(value)

                    record_dict.update({selected_columns[index]: value})
                results.append(record_dict)
            print(f'iter: {time.time() - qis}')
            # s3_client.upload_part(
            #     Body=
            # )
        curs.close()
    finally:
        if curs and not curs.closed:
            curs.close()
        db_conn.close()

    return results


if __name__ == '__main__':
    pass
