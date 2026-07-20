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
  },
  "output": {
    "mode": "s3_batches",
    "batch_size": 2000
  }
}
```
 - `rds_config`: Block required to contain the query items.
 - `records`: The Cumulus database table name to get records for (providers, collections, rules, granules, executions, async_operations, pdrs).
 - `columns`: The columns to request from the database `"column_1, column_2"`. This will default to `*` if nothing is provided. 
 - `<table>_where`: A Postgresql compliant where clause can be provided when querying for granules. A specific table prefix must be provided (granules, providers, collections, pdrs, files, executions). More than one can be supplied: https://nasa.github.io/cumulus/docs/architecture/#postgresql-database-schema-diagram
   - `"granules_where": ""`
   - `"files_where": ""`
 - `where`: A Postgresql compliant where clause to be provided when querying for non-granule records (collections, providers, etc.)
   - `"where": "provider_name LIKE '%value'"`
   - `"where": "collection_name='rssmif17d3d___7'"`.
 - `limit`: The number of records to return. A value should be supplied sufficient for the expected results. A default of 10 will be used if not supplied.
 - `output`: Block specifying output mode.
   - `mode`: Output mode requested. Currently supported modes:
     - `multipart_s3` (default): Existing multipart S3 behavior and the default when no output mode is specified.
     - `single_s3`: Writes the complete result to a single S3 object.
     - `s3_batches`: Writes query results to separate S3 objects containing a configurable number of rows.
   - `batch_size`: Number of rows to include in each batch when `s3_batches` is selected (default 10,000).
 - `is_test`: If true, the code will not be run as a `cumulus_task` and the input event will not go through the CMA.

The `columns`, `where`, and `limit` keys are optional. 

The lambda returns a dictionary with the following format:
```json
{
  "output_mode": "multipart_s3",
  "bucket": "bucket-name",
  "key": "rds_lambda/query_results_1694108903180410167.json",
  "count": 113192,
  "records": "granules"
}
```
 - `output_mode`: The mode in which the output was delivered.
 - `bucket`: The bucket where the results are stored.
 - `key`: The S3 key of the results file. The numerical string is a epoch nanosecond value to prevent overwriting query results.
 - `count`: The number of records stored in the results file.
 - `records`: The Cumulus data table from which records were returned (should correspond to `records` in request event).

When `s3_batches` output is requested, the lambda returns these additional keys:
 - `prefix`: S3 prefix containing all batch output.
 - `manifest_key`: Full S3 key to output manifest.
 - `batch_count`: Number of returned batches.

For example:
```json
{
    "output_mode": "s3_batches",
    "bucket": "bucket-name",
    "prefix": "rds_lambda/query_results_1784582169647533246/",
    "manifest_key": "rds_lambda/query_results_1784582169647533246/manifest.json",
    "batch_count": 5,
    "count": 10000,
    "records": "granules"
}
```

The batch `manifest.json` contains a list of JSON files for each batch:
```json
{
    "status": "complete",
    "total_rows": 10000,
    "batch_count": 5,
    "batches": [
        {
            "number": 1,
            "key": "rds_lambda/query_results_1784582169647533246/batch-000001.json",
            "row_count": 2000,
            "byte_count": 347751
        },
       "..."
    ]
}
```
- `status`: `complete` if output is complete.
- `total_rows`: Total number of rows returned.
- `batch_count`: Number of batches returned.
- `batches`: List of individual batches.
  - `number`: Batch number.
  - `key`: S3 key to individual batch output.
  - `row_count`: Number of rows in batch.
  - `byte_count`: Total number of bytes contained in batch.