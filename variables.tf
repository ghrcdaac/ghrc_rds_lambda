variable "aws_profile" {
  type    = string
  default = "SBX"
}

variable "cumulus_lambda_role_arn" {
  type = string
}

variable "cumulus_lambda_role_name" {
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

variable "prefix" {
  type = string
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "s3_bucket_name" {
  type = string
}

variable "s3_key_prefix" {
  description = "Path to lookup file"
  default     = "discover-granule/lookup"
}

variable "timeout" {
  description = "Lambda function time-out"
  default     = 900
}

variable "cumulus_user_credentials_secret_arn" {
  type = string
}
