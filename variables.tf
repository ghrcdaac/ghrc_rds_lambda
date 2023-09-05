variable "aws_profile" {
  type    = string
  default = "SBX"
}
variable "aws_decrypt_key_arn" {
  type = string
}

variable "cumulus_lambda_role_arn" {
  type = string
}

variable "cumulus_lambda_role_name" {
  type = string
}

variable "cumulus_message_adapter_dir" {
  type = string
  default = null
}

variable "cumulus_user_credentials_secret_arn" {
  type = string
}

variable "env_variables" {
  type    = map(string)
  default = {}
}

variable "security_group_ids" {
  type    = list(string)
  default = null
}

variable "subnet_ids" {
  type    = list(string)
  default = null
}

variable "layers" {
  type    = list(string)
  default = []
}

variable "memory_size" {
  description = "Lambda RAM limit"
  default     = 2048
}

variable "stack_prefix" {
  type = string
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "s3_bucket_name" {
  description = "Bucket to store query results."
  type = string
}

variable "s3_key_prefix" {
  description = "S3 key prefix for query results. If this is changed it should likely end with a \"/\". See https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html"
  default     = "rds_lambda/"
}

variable "timeout" {
  description = "Lambda function time-out"
  default     = 900
}


