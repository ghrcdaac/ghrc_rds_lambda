# GHRC RDS Lambda

## Overview
This module provides direct read-only access to the Cumulus AWS RDS database. It 
can be used to query records much more quickly than using the Cumulus API. Retrieved results
are stored in S3.

## Configuration
This lambda can be used by itself once deployed or it can be integrated into a workflow that uses Cumulus.

### Terraform
This is an example of a terraform module configuration block for the lambda:
```terraform
module "ghrc_rds_lambda"  {
  source      = "https://github.com/ghrcdaac/ghrc_rds_lambda/releases/download/<release_version>/ghrc_rds_lambda.zip"
  stack_prefix = var.prefix
  region       = var.region
  layers       = [aws_lambda_layer_version.cma-python.arn]
  memory_size  = 2048
  timeout      = 900

  aws_decrypt_key_arn                 = module.cumulus.provider_kms_key_id
  cumulus_lambda_role_arn             = module.cumulus.lambda_processing_role_arn
  cumulus_lambda_role_name            = module.cumulus.lambda_processing_role_name
  cumulus_message_adapter_dir         = local.CUMULUS_MESSAGE_ADAPTER_DIR
  cumulus_user_credentials_secret_arn = data.terraform_remote_state.data_persistence.outputs.user_credentials_secret_arn
  s3_bucket_name                      = lookup(var.buckets.internal, "name", null)

  subnet_ids         = module.ngap.ngap_subnets_ids
  security_group_ids = [
    aws_security_group.no_ingress_all_egress.id,
    data.terraform_remote_state.data_persistence.outputs.rds_security_group
  ]
}
```

### Building and Deploying Lambda Package
The `build_and_deploy.sh` script can be used to to locally build and deploy an updated lambda package once the terraform module has been deployed. Ensure that you have setup an `env.sh` with the required values. See the `env.sh.example` file.

## Querying
The code imposes some restrictions on the type of query that can be built and run on the lambda. Firstly, the cursor
is used as read-only. Secondly, there is a simplified DSL for querying that restricts what can be passed to the query
builder.

Below is an example AWS lambda test event that shows the format of the event that is expected:
```json
{
  "is_test": true,
  "rds_config": {
    "records": "",
    "columns": "",
    "<table>_where": "",
    "where": "",
    "limit": 10
  }
}
```
 - `rds_config`: Block required to contain the query items.
 - `records`: The Cumulus database table name to get records for (providers, collections, rules, granules, executions, async_operations, pdrs).
 - `columns`: The columns to request from the database `"column_1, column_2"`. This will default to `*` if nothing is provided. 
 - `<table>_where`: A Postgresql compliant where clause can be provided when querying for granules. A specific table prefix must be provided (granules, providers, collections, pdrs, files, executions). More than one can be supplied: https://nasa.github.io/cumulus/docs/architecture/#postgresql-database-schema-diagram
   - `"granules_where": ""`
   - `"files_where": ""`
 - `"where"`: A Postgresql compliant where clause to be provided when querying for non-granule records (collections, providers, etc.)
   - `"where": "provider_name LIKE '%value'"`
   - `"where": "collection_name='rssmif17d3d___7'"`.
 - `limit`: The number of records to return. A value should be supplied sufficient for the expected results. A default of 10 will be used if not supplied.
 - `is_test`: If true, the code will not be run as a `cumulus_task` and the input event will not go through the CMA.

The `columns`, `where`, and `limit` keys are optional. 

The lambda returns a dictionary with the following format:
```json
{
  "bucket": "prefix-name",
  "key": "rds_lambda/query_results_1694108903180410167.json",
  "count": 113192
}
```
 - `bucket`: The bucket where the results are stored.
 - `key`: The S3 key of the results file. The numerical string is a epoc nanosecond value to prevent overwriting query results.
 - `count`: The number of records stored in the results file.
