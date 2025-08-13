locals {
  analytics_views = {
    healthcare_cost_summary = {
      description = "Healthcare cost analysis and savings metrics"
      sql_file    = "${path.module}/../sql/views/healthcare_cost_summary.sql"
    }
    network_cost_analysis = {
      description = "In-network vs out-of-network performance comparison"
      sql_file    = "${path.module}/../sql/views/network_cost_analysis.sql"
    }
    top_cost_saving_providers = {
      description = "Providers generating highest cost savings"
      sql_file    = "${path.module}/../sql/views/top_cost_saving_providers.sql"
    }
    monthly_claims_trends = {
      description = "Monthly volume and cost trends over time"
      sql_file    = "${path.module}/../sql/views/monthly_claims_trends.sql"
    }
    executive_dashboard = {
      description = "Key performance indicators for executive reporting"
      sql_file    = "${path.module}/../sql/views/executive_dashboard.sql"
    }
    high_cost_claimants = {
      description = "High-cost patients for case management"
      sql_file    = "${path.module}/../sql/views/high_cost_claimants.sql"
    }
    claims_amount_distribution = {
      description = "Distribution analysis of claims by cost ranges"
      sql_file    = "${path.module}/../sql/views/claims_amount_distribution.sql"
    }
  }
}

resource "aws_athena_named_query" "analytics_views" {
  for_each = local.analytics_views

  name        = each.key
  description = each.value.description
  database    = aws_glue_catalog_database.main.name
  workgroup   = aws_athena_workgroup.main.name
  query       = file(each.value.sql_file)

  # Note: aws_athena_named_query does not support tags
}

# Data source to read all view files for validation
data "local_file" "view_files" {
  for_each = local.analytics_views
  filename = each.value.sql_file
}

# Output the created view queries for reference
output "analytics_views_created" {
  description = "List of analytics views created in Athena"
  value = {
    for view_name, view_config in local.analytics_views : view_name => {
      description = view_config.description
      query_name  = aws_athena_named_query.analytics_views[view_name].name
      database    = aws_athena_named_query.analytics_views[view_name].database
      workgroup   = aws_athena_named_query.analytics_views[view_name].workgroup
    }
  }
}

# CloudWatch Dashboard for Analytics Usage (Optional)
resource "aws_cloudwatch_dashboard" "analytics_usage" {
  dashboard_name = "${local.resource_prefix}-analytics-usage"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/Athena", "QueryExecutionTime", "WorkGroup", aws_athena_workgroup.main.name],
            ["AWS/Athena", "ProcessedBytes", "WorkGroup", aws_athena_workgroup.main.name],
            ["AWS/Athena", "QueryQueueTime", "WorkGroup", aws_athena_workgroup.main.name]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 300
          title   = "Athena Query Performance"
        }
      }
    ]
  })

  # Note: aws_cloudwatch_dashboard does not support tags in this provider version
}

# S3 bucket policy for view deployment artifacts
resource "aws_s3_object" "view_deployment_readme" {
  bucket = aws_s3_bucket.glue_assets.bucket
  key    = "analytics-views/README.md"
  
  content = <<-EOT
# Analytics Views Deployment

This folder contains deployment artifacts for analytics views.

## Views Available:
${join("\n", [for view_name, view_config in local.analytics_views : "- **${view_name}**: ${view_config.description}"])}

## Usage:
1. Views are automatically deployed via Terraform
2. Query views directly: `SELECT * FROM ${aws_glue_catalog_database.main.name}.view_name`
3. Named queries available in Athena console under "${aws_athena_workgroup.main.name}" workgroup

## Monitoring:
- CloudWatch Dashboard: ${aws_cloudwatch_dashboard.analytics_usage.dashboard_name}
- View performance metrics tracked automatically
EOT

  tags = merge(local.common_tags, {
    Type = "Documentation"
  })
}