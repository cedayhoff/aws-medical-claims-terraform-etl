# Medical Claim Sample ETL

## Prerequisites
## medical_claims_data.csv is randomly generated data.  Not actual or original data.
### Create Lambda Deployment Package
```bash
cd scripts/lambda
python -m zipfile -c ../lambda.zip lambda_function.py
cd ../..
```

## Infrastructure Deployment

### 1. Deploy Infrastructure (Includes Automatic Script Upload)

**Development Environment:**
```bash
cd terraform
terraform init
terraform workspace select dev || terraform workspace new dev
terraform plan -var="environment=dev" -var="aws_region=us-east-2"
terraform apply -var="environment=dev" -var="aws_region=us-east-2" -auto-approve
cd ..
```

**Staging Environment:**
```bash
cd terraform
terraform workspace select staging || terraform workspace new staging
terraform plan -var="environment=staging" -var="aws_region=us-east-2"
terraform apply -var="environment=staging" -var="aws_region=us-east-2" -auto-approve
cd ..
```

**Production Environment:**
```bash
cd terraform
terraform workspace select production || terraform workspace new production
terraform plan -var="environment=production" -var="aws_region=us-east-2"
terraform apply -var="environment=production" -var="aws_region=us-east-2" -auto-approve
cd ..
```

### 2. Upload Data

**For Development:**
```bash
aws s3 cp medical_claims_data.csv s3://medclaims-dev-raw-data/raw/ --region us-east-2
```

**For Staging:**
```bash
aws s3 cp medical_claims_data.csv s3://medclaims-staging-raw-data/raw/ --region us-east-2
```

**For Production:**
```bash
aws s3 cp medical_claims_data.csv s3://medclaims-production-raw-data/raw/ --region us-east-2
```

### 3. Run ETL Pipeline

**Development - PowerShell:**
```powershell
aws stepfunctions start-execution --state-machine-arn arn:aws:states:us-east-2:505906471694:stateMachine:medclaims-dev-etl-pipeline --name dev-run-$(Get-Date -UFormat %s) --region us-east-2
```

**Staging - PowerShell:**
```powershell
aws stepfunctions start-execution --state-machine-arn arn:aws:states:us-east-2:505906471694:stateMachine:medclaims-staging-etl-pipeline --name staging-run-$(Get-Date -UFormat %s) --region us-east-2
```

**Production - PowerShell:**
```powershell
aws stepfunctions start-execution --state-machine-arn arn:aws:states:us-east-2:505906471694:stateMachine:medclaims-production-etl-pipeline --name production-run-$(Get-Date -UFormat %s) --region us-east-2
```

### 4. Deploy Analytics Views

**Development:**
```bash
./scripts/deployment/deploy-views.sh dev us-east-2
```

**Staging:**
```bash
./scripts/deployment/deploy-views.sh staging us-east-2
```

**Production:**
```bash
./scripts/deployment/deploy-views.sh production us-east-2
```

## Validation Queries

### Test Data Processing
```bash
aws athena start-query-execution \
  --work-group medclaims-dev-workgroup \
  --query-string "SELECT COUNT(*) FROM fact_claims" \
  --query-execution-context Database=medclaims-dev_db \
  --region us-east-2 \
  --result-configuration OutputLocation=s3://medclaims-dev-glue-assets/athena-results/
```

### Test Analytics Views
```bash
aws athena start-query-execution \
  --work-group medclaims-dev-workgroup \
  --query-string "SELECT * FROM executive_dashboard" \
  --query-execution-context Database=medclaims-dev_db \
  --region us-east-2 \
  --result-configuration OutputLocation=s3://medclaims-dev-glue-assets/athena-results/
```

## Teardown

**Development:**
```bash
cd terraform
terraform workspace select dev
terraform destroy -var="environment=dev" -var="aws_region=us-east-2" -auto-approve
```

**Staging:**
```bash
cd terraform
terraform workspace select staging
terraform destroy -var="environment=staging" -var="aws_region=us-east-2" -auto-approve
```

**Production:**
```bash
cd terraform
terraform workspace select production
terraform destroy -var="environment=production" -var="aws_region=us-east-2" -auto-approve
```

## Automated Deployment (GitHub Actions)

After initial setup, view updates are automated via GitHub Actions:

### Setup GitHub Secrets
```bash
# Required secrets in GitHub repository settings:
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_ACCESS_KEY_ID_STAGING=<staging-access-key>
AWS_SECRET_ACCESS_KEY_STAGING=<staging-secret-key>
AWS_ACCESS_KEY_ID_PROD=<prod-access-key>
AWS_SECRET_ACCESS_KEY_PROD=<prod-secret-key>
```

### Automated Triggers
- **Push to main/master**: Deploys views through dev → staging → production
- **SQL file changes**: Triggers validation and deployment
- **Terraform changes**: Updates infrastructure and views

### Manual GitHub Actions
```bash
# Trigger deployment manually via GitHub UI:
# Actions → Deploy Analytics Views → Run workflow
```

## Configuration
- Account ID: x
- Region: us-east-2
- Environment: dev
- Expected records: x