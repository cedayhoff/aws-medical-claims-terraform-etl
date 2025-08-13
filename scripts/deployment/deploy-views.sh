#!/bin/bash

set -e

# Configuration
ENVIRONMENT="${1:-dev}"
AWS_REGION="${2:-us-east-2}"
DATABASE_NAME="medclaims-${ENVIRONMENT}_db"
WORKGROUP_NAME="medclaims-${ENVIRONMENT}-workgroup"
VIEWS_DIR="$(dirname "$0")/../../sql/views"

echo "Starting Analytics Views Deployment"
echo "Environment: $ENVIRONMENT"
echo "Region: $AWS_REGION"
echo "Database: $DATABASE_NAME"
echo "Workgroup: $WORKGROUP_NAME"
echo "Views Directory: $VIEWS_DIR"
echo "==========================================="

# Check if views directory exists
if [ ! -d "$VIEWS_DIR" ]; then
    echo "ERROR: Views directory not found: $VIEWS_DIR"
    exit 1
fi

# Function to execute Athena query
execute_athena_query() {
    local query_file="$1"
    local view_name=$(basename "$query_file" .sql)
    
    echo "Deploying view: $view_name"
    
    # Read SQL content
    local sql_content=$(cat "$query_file")
    
    # Execute query in Athena
    local execution_id=$(aws athena start-query-execution \
        --region "$AWS_REGION" \
        --work-group "$WORKGROUP_NAME" \
        --query-execution-context Database="$DATABASE_NAME" \
        --result-configuration OutputLocation="s3://medclaims-${ENVIRONMENT}-glue-assets/athena-results/view-deployment/" \
        --query-string "$sql_content" \
        --query 'QueryExecutionId' \
        --output text)
    
    echo "   Query ID: $execution_id"
    
    # Wait for completion
    local status="RUNNING"
    local attempts=0
    local max_attempts=30
    
    while [ "$status" = "RUNNING" ] || [ "$status" = "QUEUED" ]; do
        if [ $attempts -ge $max_attempts ]; then
            echo "TIMEOUT: Query took too long to complete"
            return 1
        fi
        
        sleep 2
        attempts=$((attempts + 1))
        
        status=$(aws athena get-query-execution \
            --region "$AWS_REGION" \
            --query-execution-id "$execution_id" \
            --query 'QueryExecution.Status.State' \
            --output text)
        
        echo "   Status: $status (attempt $attempts/$max_attempts)"
    done
    
    if [ "$status" = "SUCCEEDED" ]; then
        echo "SUCCESS: View $view_name deployed successfully"
        return 0
    else
        echo "FAILED: View $view_name deployment failed"
        # Get error details
        aws athena get-query-execution \
            --region "$AWS_REGION" \
            --query-execution-id "$execution_id" \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text
        return 1
    fi
}

# Function to validate view exists
validate_view() {
    local view_name="$1"
    echo "Validating view: $view_name"
    
    local validation_query="SELECT COUNT(*) as row_count FROM $view_name LIMIT 1"
    
    local execution_id=$(aws athena start-query-execution \
        --region "$AWS_REGION" \
        --work-group "$WORKGROUP_NAME" \
        --query-execution-context Database="$DATABASE_NAME" \
        --result-configuration OutputLocation="s3://medclaims-${ENVIRONMENT}-glue-assets/athena-results/view-validation/" \
        --query-string "$validation_query" \
        --query 'QueryExecutionId' \
        --output text)
    
    # Wait for completion
    sleep 5
    
    local status=$(aws athena get-query-execution \
        --region "$AWS_REGION" \
        --query-execution-id "$execution_id" \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$status" = "SUCCEEDED" ]; then
        echo "VALIDATION SUCCESS: View $view_name is queryable"
        return 0
    else
        echo "VALIDATION FAILED: View $view_name is not accessible"
        return 1
    fi
}

# Main deployment process
main() {
    echo "📋 Found view files:"
    find "$VIEWS_DIR" -name "*.sql" -type f | sort
    echo ""
    
    local success_count=0
    local total_count=0
    local failed_views=""
    
    # Deploy each view
    for view_file in "$VIEWS_DIR"/*.sql; do
        if [ -f "$view_file" ]; then
            total_count=$((total_count + 1))
            view_name=$(basename "$view_file" .sql)
            
            echo "==========================================="
            if execute_athena_query "$view_file"; then
                if validate_view "$view_name"; then
                    success_count=$((success_count + 1))
                else
                    failed_views="$failed_views $view_name"
                fi
            else
                failed_views="$failed_views $view_name"
            fi
            echo ""
        fi
    done
    
    # Deployment summary
    echo "==========================================="
    echo "📊 DEPLOYMENT SUMMARY"
    echo "Total views: $total_count"
    echo "Successful: $success_count"
    echo "Failed: $((total_count - success_count))"
    
    if [ -n "$failed_views" ]; then
        echo "Failed views:$failed_views"
        echo ""
        echo "Troubleshooting:"
        echo "1. Check Athena console for detailed error messages"
        echo "2. Verify base tables exist: fact_claims, dim_claimant, dim_provider, dim_date"
        echo "3. Ensure workgroup and database are accessible"
        echo "4. Check AWS credentials and permissions"
        exit 1
    else
        echo "ALL VIEWS DEPLOYED SUCCESSFULLY!"
        echo ""
        echo "Access analytics:"
        echo "- Athena Console: https://${AWS_REGION}.console.aws.amazon.com/athena/"
        echo "- Database: $DATABASE_NAME"
        echo "- Workgroup: $WORKGROUP_NAME"
        echo ""
        echo "Example queries:"
        echo "SELECT * FROM healthcare_cost_summary;"
        echo "SELECT * FROM executive_dashboard;"
        echo "SELECT * FROM network_cost_analysis;"
    fi
}

# Pre-flight checks
echo "🔍 Pre-flight checks..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI not found. Please install AWS CLI."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity --region "$AWS_REGION" &> /dev/null; then
    echo "ERROR: AWS credentials not configured or invalid."
    exit 1
fi

echo "Pre-flight checks passed"
echo ""

# Run main deployment
main

echo "🏁 Deployment complete!"