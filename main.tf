locals {
  default_tags = {
    Deployment = var.prefix
  }
}

resource "aws_lambda_function" "rds_lambda" {
  function_name = "${var.prefix}-rds-lambda"
  source_code_hash = filebase64sha256("${path.module}/rds_package.zip")
  handler = "task.lambda_function.handler"
  runtime = "python3.8"
  filename = "${path.module}/rds_package.zip"
  role = var.cumulus_lambda_role_arn
  timeout = var.timeout
  memory_size = var.memory_size
  tags = local.default_tags

  environment {
    variables = merge({
      bucket_name = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      cumulus_credentials_arn = var.cumulus_user_credentials_secret_arn
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
