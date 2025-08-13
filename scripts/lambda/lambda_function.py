import json
import boto3
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    try:
        processed_bucket = os.environ['PROCESSED_BUCKET']
        
        # Initialize data quality metrics
        quality_metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'bucket': processed_bucket,
            'checks_performed': [],
            'issues_found': [],
            'overall_status': 'PASS'
        }
        
        # Check 1: Verify bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=processed_bucket)
            quality_metrics['checks_performed'].append('bucket_accessibility')
            logger.info(f"Bucket {processed_bucket} is accessible")
        except Exception as e:
            quality_metrics['issues_found'].append(f"Bucket access error: {str(e)}")
            quality_metrics['overall_status'] = 'FAIL'
        
        # Check 2: Verify processed data files exist
        try:
            response = s3_client.list_objects_v2(
                Bucket=processed_bucket,
                Prefix='processed/'
            )
            
            file_count = response.get('KeyCount', 0)
            quality_metrics['checks_performed'].append('file_existence_check')
            quality_metrics['file_count'] = file_count
            
            if file_count == 0:
                quality_metrics['issues_found'].append("No processed files found")
                quality_metrics['overall_status'] = 'WARN'
            else:
                logger.info(f"Found {file_count} processed files")
                
        except Exception as e:
            quality_metrics['issues_found'].append(f"File listing error: {str(e)}")
            quality_metrics['overall_status'] = 'FAIL'
        
        # Check 3: Verify recent data (files moDified in last 24 hours)
        try:
            recent_files = 0
            if 'Contents' in response:
                current_time = datetime.utcnow()
                for obj in response['Contents']:
                    file_age = (current_time - obj['LastModified'].replace(tzinfo=None)).total_seconds()
                    if file_age < 86400:
                        recent_files += 1
            
            quality_metrics['checks_performed'].append('data_freshness_check')
            quality_metrics['recent_files'] = recent_files
            
            if recent_files == 0:
                quality_metrics['issues_found'].append("No recent data files found (within 24 hours)")
                if quality_metrics['overall_status'] != 'FAIL':
                    quality_metrics['overall_status'] = 'WARN'
            else:
                logger.info(f"Found {recent_files} recent files")
                
        except Exception as e:
            quality_metrics['issues_found'].append(f"Data freshness check error: {str(e)}")
            quality_metrics['overall_status'] = 'FAIL'
        
        # Check 4: Sample file validation (check if files are not empty)
        try:
            if 'Contents' in response and len(response['Contents']) > 0:
                sample_file = response['Contents'][0]
                file_size = sample_file['Size']
                
                quality_metrics['checks_performed'].append('file_size_validation')
                quality_metrics['sample_file_size'] = file_size
                
                if file_size == 0:
                    quality_metrics['issues_found'].append("Sample file is empty")
                    quality_metrics['overall_status'] = 'FAIL'
                else:
                    logger.info(f"Sample file size: {file_size} bytes")
                    
        except Exception as e:
            quality_metrics['issues_found'].append(f"File size validation error: {str(e)}")
            quality_metrics['overall_status'] = 'FAIL'
        
        # Log final results
        logger.info(f"Data quality check completed. Status: {quality_metrics['overall_status']}")
        if quality_metrics['issues_found']:
            logger.warning(f"Issues found: {quality_metrics['issues_found']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(quality_metrics, indent=2)
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in data quality check: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'overall_status': 'ERROR'
            })
        }