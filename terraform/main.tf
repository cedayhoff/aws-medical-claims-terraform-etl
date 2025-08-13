terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "medclaims"
}

# Local variables
locals {
  resource_prefix = "${var.project_name}-${var.environment}"
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
    Purpose     = "Medical Claims Analytics"
  }
}

# Data for current AWS account
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 Buckets for Data Lake
resource "aws_s3_bucket" "raw_data" {
  bucket        = "${local.resource_prefix}-raw-data"
  force_destroy = true
  tags          = local.common_tags
}

resource "aws_s3_bucket" "processed_data" {
  bucket        = "${local.resource_prefix}-processed-data"
  force_destroy = true
  tags          = local.common_tags
}

resource "aws_s3_bucket" "curated_data" {
  bucket        = "${local.resource_prefix}-curated-data"
  force_destroy = true
  tags          = local.common_tags
}

resource "aws_s3_bucket" "glue_assets" {
  bucket        = "${local.resource_prefix}-glue-assets"
  force_destroy = true
  tags          = local.common_tags
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# VPC and Redshift components removed for assessment - using Athena for querying

# Redshift cluster and networking removed for assessment

# IAM Role for Glue
resource "aws_iam_role" "glue" {
  name = "${local.resource_prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.resource_prefix}-glue-s3-policy"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.raw_data.arn}/*",
          "${aws_s3_bucket.processed_data.arn}/*",
          "${aws_s3_bucket.curated_data.arn}/*",
          "${aws_s3_bucket.glue_assets.arn}/*",
          aws_s3_bucket.raw_data.arn,
          aws_s3_bucket.processed_data.arn,
          aws_s3_bucket.curated_data.arn,
          aws_s3_bucket.glue_assets.arn
        ]
      }
    ]
  })
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name        = "${local.resource_prefix}_db"
  description = "Medical claims analytics database"
}

# Glue Job for Incremental Raw Data Processing
# Upload Glue scripts to S3
resource "aws_s3_object" "raw_processor_script" {
  bucket = aws_s3_bucket.glue_assets.bucket
  key    = "scripts/process_raw_data.py"
  source = "${path.module}/../glue/process_raw_data.py"
  etag   = filemd5("${path.module}/../glue/process_raw_data.py")
  tags   = local.common_tags
}

resource "aws_s3_object" "dimensional_processor_script" {
  bucket = aws_s3_bucket.glue_assets.bucket
  key    = "scripts/create_dimensional_model.py"
  source = "${path.module}/../glue/create_dimensional_model.py"
  etag   = filemd5("${path.module}/../glue/create_dimensional_model.py")
  tags   = local.common_tags
}

resource "aws_glue_job" "raw_processor" {
  name              = "${local.resource_prefix}-incremental-processor"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/${aws_s3_object.raw_processor_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--SOURCE_BUCKET"                    = aws_s3_bucket.raw_data.bucket
    "--TARGET_BUCKET"                    = aws_s3_bucket.processed_data.bucket
  }

  tags = local.common_tags
}

# Glue Job for Dimensional Processing
resource "aws_glue_job" "dimensional_processor" {
  name              = "${local.resource_prefix}-dimensional-processor"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/${aws_s3_object.dimensional_processor_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--SOURCE_BUCKET"                    = aws_s3_bucket.processed_data.bucket
    "--TARGET_BUCKET"                    = aws_s3_bucket.curated_data.bucket
  }

  tags = local.common_tags
}

# Fact table creation is handled by the dimensional processor job

# IAM Role for Step Functions
resource "aws_iam_role" "step_functions" {
  name = "${local.resource_prefix}-stepfunctions-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.resource_prefix}-stepfunctions-policy"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "*"
      }
    ]
  })
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${local.resource_prefix}-etl-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/../step-functions/etl-pipeline.json", {
    data_quality_lambda_arn      = aws_lambda_function.data_quality.arn
    raw_processor_job_name       = aws_glue_job.raw_processor.name
    dimensional_processor_job_name = aws_glue_job.dimensional_processor.name
    raw_data_bucket             = aws_s3_bucket.raw_data.bucket
    processed_data_bucket       = aws_s3_bucket.processed_data.bucket
    curated_data_bucket         = aws_s3_bucket.curated_data.bucket
  })

  tags = local.common_tags
}

# Lambda for Data Quality Checks
resource "aws_iam_role" "lambda" {
  name = "${local.resource_prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "${local.resource_prefix}-lambda-s3-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.processed_data.arn}/*",
          aws_s3_bucket.processed_data.arn
        ]
      }
    ]
  })
}

resource "aws_lambda_function" "data_quality" {
  function_name = "${local.resource_prefix}-data-quality"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  role          = aws_iam_role.lambda.arn
  timeout       = 60
  memory_size   = 256

  filename         = "${path.module}/../scripts/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../scripts/lambda.zip")

  environment {
    variables = {
      PROCESSED_BUCKET = aws_s3_bucket.processed_data.bucket
    }
  }

  tags = local.common_tags
}

# EventBridge scheduling removed for assessment - manual ETL triggering

# EventBridge IAM roles removed for assessment

# Athena for Querying
resource "aws_athena_database" "main" {
  name          = "${replace(local.resource_prefix, "-", "_")}_athena_db"
  bucket        = aws_s3_bucket.glue_assets.bucket
  force_destroy = true
}

resource "aws_athena_workgroup" "main" {
  name          = "${local.resource_prefix}-workgroup"
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.glue_assets.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${local.resource_prefix}"
  retention_in_days = 7
  skip_destroy      = false
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${local.resource_prefix}"
  retention_in_days = 7
  skip_destroy      = false
  tags              = local.common_tags
}

# SNS notifications removed for assessment - use CloudWatch logs instead

# Outputs
output "deployment_summary" {
  description = "Complete deployment information with URLs and access details"
  value = {
    # Infrastructure Details
    region = data.aws_region.current.name
    account_id = data.aws_caller_identity.current.account_id
    environment = var.environment
    
    # S3 Data Lake URLs
    data_buckets = {
      raw_data_bucket = aws_s3_bucket.raw_data.bucket
      raw_data_url = "https://s3.console.aws.amazon.com/s3/buckets/${aws_s3_bucket.raw_data.bucket}"
      processed_data_bucket = aws_s3_bucket.processed_data.bucket
      processed_data_url = "https://s3.console.aws.amazon.com/s3/buckets/${aws_s3_bucket.processed_data.bucket}"
      curated_data_bucket = aws_s3_bucket.curated_data.bucket
      curated_data_url = "https://s3.console.aws.amazon.com/s3/buckets/${aws_s3_bucket.curated_data.bucket}"
      glue_assets_bucket = aws_s3_bucket.glue_assets.bucket
      glue_assets_url = "https://s3.console.aws.amazon.com/s3/buckets/${aws_s3_bucket.glue_assets.bucket}"
    }
    
    # ETL Processing URLs
    etl_services = {
      step_functions_arn = aws_sfn_state_machine.etl_pipeline.arn
      step_functions_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/states/home?region=${data.aws_region.current.name}#/statemachines/view/${aws_sfn_state_machine.etl_pipeline.arn}"
      glue_jobs_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/glue/home?region=${data.aws_region.current.name}#/v2/etl-jobs"
      raw_processor_job = aws_glue_job.raw_processor.name
      dimensional_processor_job = aws_glue_job.dimensional_processor.name
    }
    
    # Data Catalog URLs
    data_catalog = {
      glue_database = aws_glue_catalog_database.main.name
      glue_catalog_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/glue/home?region=${data.aws_region.current.name}#/v2/data-catalog/databases/view/${aws_glue_catalog_database.main.name}"
      athena_workgroup = aws_athena_workgroup.main.name
      athena_query_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/athena/home?region=${data.aws_region.current.name}#/query-editor"
    }
    
    # Monitoring URLs
    monitoring = {
      cloudwatch_logs_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#logsV2:log-groups"
      glue_jobs_logs = "/aws-glue/jobs/${local.resource_prefix}"
      step_functions_logs = "/aws/vendedlogs/states/${local.resource_prefix}"
      lambda_function = aws_lambda_function.data_quality.function_name
      lambda_logs_url = "https://${data.aws_region.current.name}.console.aws.amazon.com/lambda/home?region=${data.aws_region.current.name}#/functions/${aws_lambda_function.data_quality.function_name}"
    }
    
    # Testing Commands
    test_commands = {
      upload_sample_data = "aws s3 cp medical_claims_data.csv s3://${aws_s3_bucket.raw_data.bucket}/raw/"
      run_etl_pipeline = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.etl_pipeline.arn} --name test-run-$(date +%s)"
      check_pipeline_status = "aws stepfunctions describe-execution --execution-arn <execution-arn-from-above>"
      query_results = "aws athena start-query-execution --work-group ${aws_athena_workgroup.main.name} --query-string 'SELECT * FROM ${aws_glue_catalog_database.main.name}.fact_claims LIMIT 10'"
    }
  }
}

# Individual outputs for easy reference
output "raw_data_bucket" {
  value = aws_s3_bucket.raw_data.bucket
}

output "step_functions_url" {
  description = "Direct URL to Step Functions state machine"
  value = "https://${data.aws_region.current.name}.console.aws.amazon.com/states/home?region=${data.aws_region.current.name}#/statemachines/view/${aws_sfn_state_machine.etl_pipeline.arn}"
}

output "athena_query_url" {
  description = "Direct URL to Athena query editor"
  value = "https://${data.aws_region.current.name}.console.aws.amazon.com/athena/home?region=${data.aws_region.current.name}#/query-editor"
}

output "glue_catalog_url" {
  description = "Direct URL to Glue Data Catalog"
  value = "https://${data.aws_region.current.name}.console.aws.amazon.com/glue/home?region=${data.aws_region.current.name}#/v2/data-catalog/databases/view/${aws_glue_catalog_database.main.name}"
}