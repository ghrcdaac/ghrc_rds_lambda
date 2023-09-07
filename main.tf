locals {
  default_tags = {
    Deployment = var.stack_prefix
  }
  lambda_package = "ghrc_rds_lambda_package.zip"
}

resource "aws_lambda_function" "rds_lambda" {
  function_name = "${var.stack_prefix}-rds-lambda"
  source_code_hash = filebase64sha256("${path.module}/${local.lambda_package}")
  handler = "task.lambda_handler.handler"
  runtime = "python3.8"
  filename = "${path.module}/${local.lambda_package}"
  role = var.cumulus_lambda_role_arn
  timeout = var.timeout
  memory_size = var.memory_size
  tags = local.default_tags

  environment {
    variables = merge({
      BUCKET_NAME = var.s3_bucket_name
      S3_KEY_PREFIX = var.s3_key_prefix
      CUMULUS_CREDENTIALS_ARN = var.cumulus_user_credentials_secret_arn
      CUMULUS_MESSAGE_ADAPTER_DIR = var.cumulus_message_adapter_dir
    }, var.env_variables)
  }

  vpc_config {
    security_group_ids = var.security_group_ids
    subnet_ids = var.subnet_ids
  }
}

resource "aws_iam_policy" "cumulus_secrets_manager_read" {
  policy = jsonencode(
  {
    Version = "2012-10-17"
    "Statement" = [
      {
        Effect = "Allow",
        Action = ["secretsmanager:GetSecretValue"],
        Resource = [
          var.cumulus_user_credentials_secret_arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cumulus_secrets_manager_policy_attach" {
  policy_arn = aws_iam_policy.cumulus_secrets_manager_read.arn
  role = var.cumulus_lambda_role_name
}
