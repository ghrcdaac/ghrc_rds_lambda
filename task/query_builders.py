from task.api_model import *

def condense_whitespaces(string):
    return ' '.join(string.split()).replace('\n', '\r')

def is_column_selected(selected_columns, table):
    print(f'Checking {table} table for {selected_columns}')
    ret = False
    if '*' in selected_columns:
        ret = True
    else:
        table_columns = db_dict.get(table)
        for selected_column in selected_columns:
            if '.' in selected_column:
                table_columns = (f'{table}.{column}' for column in table_columns)

            print(f'Is "{selected_column}" in {table_columns}')
            if selected_column in table_columns:
                ret = True
                break 
    return ret

def build_query_case_1(columns='*', limit=10, providers_where='', collections_where='', pdrs_where='', granules_where='', files_where='', executions_where=''):
    if columns != '*':
        columns = [column.strip() for column in columns.split(',')]
    query = ''
    join_queries = []
    cte_selects = []
    providers_query = ''
    providers_join = ''
    collections_query = ''
    collections_join = ''
    pdrs_query = ''
    pdrs_join = ''
    
    # Create providers query
    if providers_where or columns == '*' or is_column_selected(columns, 'providers'):
        if providers_where:
            providers_where = f'WHERE {providers_where}'
            providers_query = f'''
                providers_cte AS (
                    SELECT 
                        DISTINCT ON (providers.cumulus_id) providers.cumulus_id AS provider_cumulus_id,
                        name AS provider
                    FROM 
                        providers
                    {providers_where}
                )
            '''
            query = f'WITH {providers_query}'
            cte_selects.append('providers_cte.*')
            collections_join = 'JOIN providers_cte USING (provider_cumulus_id)'
        else:
            providers_query = f'''
                JOIN (
                    SELECT 
                        DISTINCT ON (providers.cumulus_id) providers.cumulus_id AS provider_cumulus_id,
                        name AS provider
                    FROM 
                        providers
                    JOIN granules_cte ON granules_cte.provider_cumulus_id = providers.cumulus_id
                ) AS provider_names USING (provider_cumulus_id)
            '''
            join_queries.append(providers_query)

    # Create collections query
    collections_query = ''
    if collections_where or columns == '*' or is_column_selected(columns, 'collections'):
        if collections_where:
            collections_where = f'WHERE {collections_where}'
            collections_query = f'''
                collections_cte AS (
                    SELECT collections.cumulus_id as collection_cumulus_id, concat(name, '___', version) AS collection_id
                    FROM collections
                    {collections_where}
                )
            '''
            collections_join = 'JOIN collections_cte USING (collection_cumulus_id)'
            if query.startswith('WITH'):
                query = f'{query}, {collections_query}'
            else:
                query = f'WITH {collections_query}'
            collections_query = ''
            collections_join = 'JOIN collections_cte USING (collection_cumulus_id)'
            cte_selects.append('collections_cte.*')
        else:
            collections_query = f'''
                JOIN (
                    SELECT 
                        DISTINCT ON (collections.cumulus_id) collections.cumulus_id as collection_cumulus_id,
                        concat(name, '___', version) AS collection_id
                    FROM 
                        collections
                    JOIN granules_cte ON granules_cte.collection_cumulus_id =  collections.cumulus_id
                ) AS collection_ids USING (collection_cumulus_id)
            '''
            join_queries.append(collections_query)

    # Create PDRS query
    pdrs_query = ''
    pdrs_join = ''
    if pdrs_where or columns == '*' or is_column_selected(columns, 'pdrs'):
        pdrs_query = f'''
            SELECT pdrs.cumulus_id as pdr_cumulus_id, name AS pdr_name
            FROM pdrs
        '''
        pdrs_join = ''
        if is_column_selected(columns, 'pdrs'):
            pass
        elif pdrs_where or collections_where or providers_where:
            if pdrs_where:
                pdrs_where = f'WHERE {pdrs_where}'
                pdrs_join = 'JOIN pdrs_cte USING (pdr_cumulus_id)'
            else:
                pdrs_join = 'LEFT JOIN pdrs_cte USING (pdr_cumulus_id)'

            providers_join = ''
            if 'providers_cte' in providers_query:
                providers_join = 'JOIN providers_cte USING (provider_cumulus_id)'

            collections_join = ''
            if 'collections_cte' in collections_query:
                collections_join = 'JOIN collections_cte USING (collection_cumulus_id)'
            
            pdrs_query = f'''
                pdrs_cte AS (
                    {pdrs_query}
                    {providers_join}
                    {collections_join}
                    {pdrs_where}
                )
            '''

            if query.startswith('WITH'):
                query = f'{query}, {pdrs_query}'
            else:
                query = f'WITH {pdrs_query}'

            pdrs_query = ''
            pdrs_join = 'JOIN pdrs_cte USING (pdr_cumulus_id)'
            cte_selects.append('pdrs_cte.*')
        else:
            pdrs_query = f'''
                LEFT JOIN (
                    {pdrs_query}
                    JOIN granules_cte ON granules_cte.pdr_cumulus_id = pdrs.cumulus_id
                ) AS pdr_names USING (pdr_cumulus_id)
            '''
            join_queries.append(pdrs_query)

            providers_join = ''
            if 'providers_cte' in providers_query:
                providers_join = 'JOIN providers_cte USING (provider_cumulus_id)'

            collections_join = ''
            if 'collections_cte' in collections_query:
                collections_join = 'JOIN collections_cte USING (collection_cumulus_id)'
            
            pdrs_join = ''
            if 'pdrs_cte' in pdrs_query:
                pdrs_join = 'JOIN pdrs_cte USING (pdr_cumulus_id)'

    if cte_selects:
        cte_selects = ', '.join(cte_selects)
        cte_selects = f', {cte_selects}'

    # Create granules query
    granules_limit = f'LIMIT {limit}'
    if files_where or executions_where:
        granules_limit = ''

    if granules_where:
        granules_where = f'WHERE {granules_where}'
    granules_cte = f'''
        granules_cte AS (
            SELECT 
                cumulus_id AS granule_cumulus_id,
                *
            FROM granules
            {providers_join}
            {collections_join}
            {pdrs_join}
            {granules_where}
            {granules_limit}
    )
    '''
    if query.startswith('WITH'):
        query = f'{query}, {granules_cte}'
    else:
        query = f'WITH {granules_cte}'

    query = f'{query} SELECT {", ".join(columns)} FROM granules_cte'

    # Create files query
    if files_where or columns == '*' or is_column_selected(columns, 'files'):        
        files_join = 'JOIN'
        if not files_where:
            files_join = 'LEFT JOIN'
        else:
            files_join = 'JOIN'
            files_where = f'WHERE {files_where}'

        files_query = f'''
            {files_join} (
                SELECT files.granule_cumulus_id, json_agg(files) AS files
                FROM granules_cte
                JOIN files USING (granule_cumulus_id)
                {files_where}
                GROUP BY files.granule_cumulus_id
            ) AS file_arrays USING (granule_cumulus_id)
        '''
        query = f'{query} {files_query}'

    # Create executions query
    if executions_where or columns == '*' or is_column_selected(columns, 'executions'):
        executions_join = 'JOIN'
        if not executions_where:
            executions_join = 'LEFT JOIN'
        else:
            executions_join = 'JOIN'
            executions_where = f'WHERE {executions_where}'

        executions_query = f'''
            {executions_join} (
                SELECT DISTINCT ON (granules_executions.granule_cumulus_id) granule_cumulus_id, url AS execution
                FROM granules_executions
                JOIN granules_cte USING (granule_cumulus_id)
                JOIN executions ON executions.cumulus_id = granules_executions.execution_cumulus_id
                {executions_where}
                ORDER BY granules_executions.granule_cumulus_id, executions.timestamp DESC
            ) AS execution USING (granule_cumulus_id)
        '''
        query = f'{query} {executions_query}'

    # Create full query
    query = f'{query} {" ".join(join_queries)} LIMIT {limit}'

    query = condense_whitespaces(query)
    
    return query

def build_query_case_2(columns='*', limit=10, providers_where='', collections_where='', pdrs_where='', granules_where='', files_where='', executions_where=''):
    # if columns != '*':
    columns = [column.strip() for column in columns.split(',')]
    query = ''
    join_queries = []
    cte_selects = []
    providers_query = ''
    providers_join = ''
    collections_query = ''
    collections_join = ''
    pdrs_query = ''
    pdrs_join = ''

    # Create files query
    if files_where or is_column_selected(columns, 'files'):
        if files_where:
            files_where = f'WHERE {files_where}'
        files_query = f'''
            SELECT files.granule_cumulus_id, json_agg(files) AS files
            FROM files
            {files_where}
            GROUP BY files.granule_cumulus_id
            '''

        if files_where:
            files_query = f'''
                files_cte AS (
                    SELECT files.granule_cumulus_id, json_agg(files) AS files
                FROM files
                    {files_where}
                    GROUP BY files.granule_cumulus_id
                    LIMIT {limit}
                )
            '''
            if query.startswith('WITH'):
                query = f'{query}, {files_query}'
            else:
                query = f'WITH {files_query}'

            cte_selects.append(files_query)
        else:
            files_query = f'''
                LEFT JOIN (
                    SELECT files.granule_cumulus_id, json_agg(files) AS files
                    FROM files
                    {files_where}
                    JOIN granules_cte USING (granule_cumulus_id)
                    GROUP BY files.granule_cumulus_id
                ) AS file_arrays USING (granule_cumulus_id)
            '''
            join_queries.append(files_query)

    # Create executions query
    if executions_where or is_column_selected(columns, 'executions'):
        if executions_where:
            executions_where = f'WHERE {executions_where}'
        executions_query = f'''
            SELECT DISTINCT ON (granules_executions.granule_cumulus_id) granule_cumulus_id, url AS execution
            FROM granules_executions
            JOIN granules_cte USING (granule_cumulus_id)
            JOIN executions ON executions.cumulus_id = granules_executions.execution_cumulus_id
            ORDER BY granules_executions.granule_cumulus_id, executions.timestamp DESC
        '''

        # Create executions_cte
        if executions_where:
            files_join = ''
            files_join = ''
            if files_where:
                files_join = f'JOIN files_cte (granule_cumulus_id)'

            executions_query = f'''
                executions_cte AS (
                    SELECT DISTINCT ON (granules_executions.granule_cumulus_id) granule_cumulus_id, url AS execution
                    FROM granules_executions
                    JOIN executions ON executions.cumulus_id = granules_executions.execution_cumulus_id
                    {files_join}
                    {executions_where}
                    ORDER BY granules_executions.granule_cumulus_id, executions.timestamp DESC
                    LIMIT {limit}
                )
            '''
            if query.startswith('WITH'):
                query = f'{query}, {executions_query}'
            else:
                query = f'WITH {executions_query}'

            cte_selects.append(executions_query)
        else: 
            # Create executions join query
            executions_join = 'JOIN'
            if not executions_where:
                executions_join = 'LEFT JOIN'
            else:
                executions_where = f'WHERE {executions_where}'

            executions_query = f'''
                {executions_join} (
                {executions_query}
                ) AS execution USING (granule_cumulus_id)
            '''
            join_queries.append(executions_query)

    # Create providers query
    if providers_where or is_column_selected(columns, 'providers'):
        if providers_where:
            providers_where = f'WHERE {providers_where}'
        providers_query = f'''
            JOIN (
                SELECT 
                    DISTINCT ON (providers.cumulus_id) providers.cumulus_id AS provider_cumulus_id,
                    name AS provider
                FROM 
                    providers
                JOIN granules_cte ON granules_cte.provider_cumulus_id = providers.cumulus_id
                {providers_where}
            ) AS provider_names USING (provider_cumulus_id)
        '''
        join_queries.append(providers_query)

    # Create collections query
    if collections_where or is_column_selected(columns, 'collections'):
        if collections_where:
            collections_where = f'WHERE {collections_where}'
        collections_query = f'''
            JOIN (
                SELECT 
                    DISTINCT ON (collections.cumulus_id) collections.cumulus_id as collection_cumulus_id,
                    concat(name, '___', version) AS collection_id
                FROM 
                    collections
                JOIN granules_cte ON granules_cte.collection_cumulus_id =  collections.cumulus_id
                {collections_where}
            ) AS collection_ids USING (collection_cumulus_id)
        '''
        join_queries.append(collections_query)

    # Create PDRs query
    if pdrs_where or is_column_selected(columns, 'pdrs'):
        if pdrs_where:
            pdrs_where = f'WHERE {pdrs_where}'
        pdrs_query = f'''
            LEFT JOIN (
                SELECT pdrs.cumulus_id as pdr_cumulus_id, name AS pdr_name
                FROM pdrs
                JOIN granules_cte ON granules_cte.pdr_cumulus_id = pdrs.cumulus_id
                {providers_where}
            ) AS pdr_names USING (pdr_cumulus_id)
        '''
        join_queries.append(pdrs_query)

    # Create granules query
    files_join = ''
    if files_where:
        files_join = 'JOIN files_cte ON files_cte.granule_cumulus_id = granules.cumulus_id'
    
    executions_join = ''
    if executions_where:
        executions_join = 'JOIN executions_cte ON executions_cte.granule_cumulus_id = granules.cumulus_id'

    if granules_where:
        granules_where = f'WHERE {granules_where}'

    alias = ''
    if len(cte_selects) == 0:
        alias = ', granules.cumulus_id AS granule_cumulus_id'

    granules_cte = f'''
        granules_cte AS (
            SELECT * {alias}
            FROM granules
            {files_join}
            {executions_join}
            {granules_where}
            LIMIT {limit}
        )
    '''
    if query.startswith('WITH'):
        query = f'{query}, {granules_cte}'
    else:
        query = f'WITH {granules_cte}'

    query = f'{query} SELECT {", ".join(columns)} FROM granules_cte {" ".join(join_queries)}'

    # Create full query
    query = f'{query} LIMIT {limit}'

    query = condense_whitespaces(query)
    
    return query