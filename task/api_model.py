

async_operations_db_columns = (
    'async_operation_id',
    'cumulus_id',
    'id',
    'description',
    'operation_type',
    'output',
    'status',
    'task_arn',
    'created_at',
    'updated_at'
)
async_dict = {
    'table': 'async_operations',
    'columns': async_operations_db_columns
}

collections_db_columns = (
    'collection_id',
    'name',
    'version',
    'sample_file_name',
    'granule_id_validation_regex',
    'granule_id_extraction_regex',
    'files',
    'process',
    'url_path',
    'duplicate_handling',
    'report_to_ems',
    'ignore_files_config_for_discovery',
    'meta',
    'tags',
    'created_at',
    'updated_at'
)
collections_dict = {
    'table': 'collections',
    'columns': collections_db_columns
}

executions_db_columns = (
    'execution',
    'cumulus_id',
    'arn',
    'async_operation_cumulus_id',
    'collection_cumulus_id',
    'parent_cumulus_id',
    'cumulus_version',
    'url',
    'status',
    'tasks',
    'error',
    'workflow_name',
    'duration',
    'original_payload'
)
executions_dict = {
    'table': 'executions',
    'columns': executions_db_columns
}

files_db_columns = (
    'files',
    'cumulus_id',
    'granule_cumulus_id',
    'created_at',
    'updated_at',
    'file_size',
    'bucket',
    'checksum_type',
    'checksum_value',
    'file_name',
    'key',
    'path',
    'source',
    'type'
)
files_dict = {
    'table': 'files',
    'columns': files_db_columns
}

granules_db_columns = (
    # 'granules.cumulus_id',
    'granule_id',
    'status',
    'files',
    # 'granules.collection_cumulus_id',
    'created_at',
    'updated_at',
    'published',
    'duration',
    'time_to_archive',
    'time_to_process',
    'product_volume',
    'error',
    'cmr_link',
    # 'granules.pdr_cumulus_id',
    # 'granules.provider_cumulus_id',
    'beginning_date_time',
    'ending_date_time',
    'last_update_date_time',
    'processing_end_date_time',
    'processing_start_date_time',
    'collection_id'
)
granules_dict = {
    'table': 'granules',
    'columns': granules_db_columns
}

pdrs_db_columns = (
    # 'cumulus_id',
    # 'collection_cumulus_id',
    # 'provider_cumulus_id',
    # 'execution_cumulus_id',
    'status',
    'name',
    'progress',
    'pan_sent',
    'pan_message',
    'stats',
    'address',
    'original_url',
    'duration',
    'timestamp',
    'created_at',
    'updated_at'
)
pdrs_dict = {
    'table': 'pdrs',
    'columns': pdrs_db_columns
}

providers_db_columns = (
    # 'cumulus_id',
    'id',
    'name',
    'protocol',
    'host',
    'port',
    'username',
    'password',
    'global_connection_limit',
    'private_key',
    'cm_key_id',
    'certificate_uri',
    'created_at',
    'updated_at',
    'allowed_redirects',
    'max_download_time'
)
providers_dict = {
    'table': 'providers',
    'columns': providers_db_columns
}

rules_db_columns = (
    # 'cumulus_id',
    'name',
    'workflow',
    # 'collection_cumulus_id',
    # 'provider_cumulus_id',
    'type',
    'enabled',
    'value',
    'arn',
    'log_event_arn',
    'execution_name_prefix',
    'payload',
    'meta',
    'tags',
    'queue_url'
)
rules_dict = {
    'table': 'rules',
    'columns': rules_db_columns
}

granule_model_fields = (
    'beginningDateTime',
    'cmrLink',
    'collectionId',
    'createdAt',
    'duration',
    'endingDateTime',
    'error',
    'execution',
    'files',
    'granuleId',
    'lastUpdateDateTime',
    'pdrName',
    'processingEndDateTime',
    'processingStartDateTime',
    'productVolume',
    'productionDateTime',
    'provider',
    'published',
    'queryFields',
    'status',
    'timeToArchive',
    'timeToPreprocess',
    'timestamp',
    'updatedAt'
)

db_dict = {
    'granules': granules_db_columns,
    'collections': collections_db_columns,
    'providers': providers_db_columns,
    'pdrs': pdrs_db_columns,
    'files': files_db_columns,
    'executions': executions_db_columns,
}

def api_field_names_to_db_column_names(field_names):
    db_column_names = []
    for field_name in field_names:
        db_column_names.append(api_field_name_to_db_column(field_name))
    
    return db_column_names

def api_field_name_to_db_column(field_name):
    db_column_name = ''
    if field_name in granule_model_fields:
        for character in field_name:
            new_character = character
            if character.isupper():
                new_character = f'_{character.lower()}'
            db_column_name = f'{db_column_name}{new_character}'
    
    print(f'{field_name} -> {db_column_name}')

    return db_column_name

def db_column_names_to_api_keys(column_names):
    api_keys = []
    for column_name in column_names:
        new_str = db_column_names_to_api_keys(column_name)
        api_keys.append(new_str)
    
    return api_keys

def db_column_name_to_api_field_name(db_column_name):
    new_str = ''
    capitalize_character = False
    for character in db_column_name:
        new_character = character
        if new_character == '_':
            capitalize_character = True
            continue
        elif capitalize_character:
            new_character = new_character.upper()
            capitalize_character = False
        else: # Case handled by new_character=character
            pass
        new_str = f'{new_str}{new_character}'
    
    if new_str not in granule_model_fields:
        new_str = ''

    return new_str


def test_column_api():
    keys = ['beginningDateTime', 'fieldTwo', 'fieldThree', 'something_else']
    print(keys)
    column_names = api_field_names_to_db_column_names(keys)
    print(column_names)
    keys = db_column_names_to_api_keys(column_names)
    print(keys)

def parse_where_clause(where):
    parsed_query = ''
    query_term = ''
    for character in where:
        if character.isalnum():
            query_term = f'{query_term}{character}'
        else:
            db_column_name = api_field_name_to_db_column(query_term)
            if db_column_name:
                query_term = db_column_name

            parsed_query = f'{parsed_query}{query_term}{character}'
            query_term = ''

    parsed_query = f'{parsed_query}{query_term}'

    return parsed_query

def test_parse_where_clause():
    query = 'SELECT * where granules.granuleId LIKE someValue AND collectionId=anotherValue'
    parsed_query = parse_where_clause(query)
    print(parsed_query)

if __name__ == '__main__':
    pass