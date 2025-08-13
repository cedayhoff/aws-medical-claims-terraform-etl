# Simplified Data Governance Implementation

# S3 Lifecycle Policies for Data Retention

# Raw data retention - 90dys
resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "raw-data-retention"
    status = "Enabled"
    
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    expiration {
      days = 90
    }
  }
}

# Processed data retention - 1 yr
resource "aws_s3_bucket_lifecycle_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    id     = "processed-data-retention"
    status = "Enabled"
    
    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# Curated Data retention - 7 years (HIPAA requirement - I assume this is still valid but would know if I would confirm in a real assignment)
resource "aws_s3_bucket_lifecycle_configuration" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id

  rule {
    id     = "curated-data-retention"
    status = "Enabled"
    
    filter {}

    transition {
      days          = 365
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 730
      storage_class = "GLACIER"
    }

    transition {
      days          = 1095
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = 2555 # 7yrs
    }
  }
}

# SNS Topics for Compliance Alerts

resource "aws_sns_topic" "data_quality_alerts" {
  name = "${local.resource_prefix}-data-quality-alerts"
  
  tags = merge(local.common_tags, {
    Purpose = "Data Quality Monitoring"
  })
}

resource "aws_sns_topic" "compliance_alerts" {
  name = "${local.resource_prefix}-compliance-alerts"
  
  tags = merge(local.common_tags, {
    Purpose = "Compliance Monitoring"
  })
}

# CloudWatch Alarms for Data Quality

# Alarm for Glue job failures
resource "aws_cloudwatch_metric_alarm" "glue_raw_processor_failures" {
  alarm_name          = "${local.resource_prefix}-raw-processor-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name        = "glue.driver.aggregate.numFailedTasks"
  namespace          = "AWS/Glue"
  period             = "300"
  statistic          = "Sum"
  threshold          = "0"
  alarm_description  = "Alert when raw processor Glue job fails"
  alarm_actions      = [aws_sns_topic.data_quality_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.raw_processor.name
    Type = "count"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_dimensional_processor_failures" {
  alarm_name          = "${local.resource_prefix}-dimensional-processor-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name        = "glue.driver.aggregate.numFailedTasks"
  namespace          = "AWS/Glue"
  period             = "300"
  statistic          = "Sum"
  threshold          = "0"
  alarm_description  = "Alert when dimensional processor Glue job fails"
  alarm_actions      = [aws_sns_topic.data_quality_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.dimensional_processor.name
    Type = "count"
  }
}

# S3 Bucket Policies for Access Control

# Bucket policy for raw data will restrict access
resource "aws_s3_bucket_policy" "raw_data_policy" {
  bucket = aws_s3_bucket.raw_data.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DenyInsecureConnections"
        Effect = "Deny"
        Principal = "*"
        Action = "s3:*"
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid    = "AllowAuthorizedAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.glue.arn,
            aws_iam_role.lambda.arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*"
        ]
      }
    ]
  })
}

# Glue Catalog Table Tags

resource "aws_glue_catalog_table" "fact_claims_governance" {
  database_name = aws_glue_catalog_database.main.name
  name          = "fact_claims_governance"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.curated_data.bucket}/curated/fact_claims/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "claim_key"
      type = "bigint"
      comment = "Surrogate key for claim"
    }
    columns {
      name = "claim_id"
      type = "string"
      comment = "PII - Natural claim identifier"
    }
    columns {
      name = "claimant_key"
      type = "int"
      comment = "Foreign key to patient dimension"
    }
    columns {
      name = "provider_key"
      type = "int"
      comment = "Foreign key to provider dimension"
    }
    columns {
      name = "received_date_key"
      type = "int"
      comment = "Foreign key to date dimension"
    }
    columns {
      name = "charge_amount"
      type = "double"
      comment = "Amount charged by provider"
    }
    columns {
      name = "allowed_amount"
      type = "double"
      comment = "Amount approved by insurance"
    }
    columns {
      name = "units"
      type = "int"
      comment = "Number of units"
    }
    columns {
      name = "cost_savings_amount"
      type = "double"
      comment = "Savings from network negotiation"
    }
    columns {
      name = "reimbursement_rate"
      type = "double"
      comment = "Percentage of charges approved"
    }
    columns {
      name = "in_network"
      type = "boolean"
      comment = "Network status indicator"
    }
    columns {
      name = "claim_type"
      type = "string"
      comment = "Type of medical claim"
    }
    columns {
      name = "claim_code"
      type = "string"
      comment = "Medical procedure code"
    }
    columns {
      name = "procedure_description"
      type = "string"
      comment = "Revenue code and procedure description"
    }
    columns {
      name = "claim_modifier_1"
      type = "string"
      comment = "Primary claim code modifier"
    }
    columns {
      name = "claim_modifier_2"
      type = "string"
      comment = "Secondary claim code modifier"
    }
    columns {
      name = "diagnosis_code_1"
      type = "string"
      comment = "Primary diagnosis code"
    }
    columns {
      name = "diagnosis_code_2"
      type = "string"
      comment = "Secondary diagnosis code"
    }
    columns {
      name = "diagnosis_code_3"
      type = "string"
      comment = "Tertiary diagnosis code"
    }
    columns {
      name = "diagnosis_code_4"
      type = "string"
      comment = "Quaternary diagnosis code"
    }
    columns {
      name = "diagnosis_code_5"
      type = "string"
      comment = "Fifth diagnosis code"
    }
  }

  parameters = {
    "classification"     = "parquet"
    "compressionType"    = "snappy"
    "typeOfData"         = "file"
    "DataClassification" = "PHI"
    "ComplianceScope"    = "HIPAA"
    "DataQuality"        = "Gold"
    "RetentionPeriod"    = "7Years"
  }
}

# Outputs for Governance Resources

output "governance_summary" {
  description = "Summary of governance controls implemented"
  value = {
    data_retention = {
      raw_data = "90 days with automatic archival"
      processed_data = "1 year with tiered storage"
      curated_data = "7 years for HIPAA compliance"
    }
    
    monitoring = {
      alerts_configured = true
      sns_topics = {
        data_quality = aws_sns_topic.data_quality_alerts.arn
        compliance = aws_sns_topic.compliance_alerts.arn
      }
      cloudwatch_alarms = [
        aws_cloudwatch_metric_alarm.glue_raw_processor_failures.alarm_name,
        aws_cloudwatch_metric_alarm.glue_dimensional_processor_failures.alarm_name
      ]
    }
    
    data_classification = {
      fact_table_tagged = true
      classification = "PHI"
      compliance_scope = "HIPAA"
    }
    
    access_control = {
      ssl_enforcement = true
      iam_based_access = true
      bucket_policies_enabled = true
    }
  }
}