import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'SOURCE_BUCKET', 
    'TARGET_BUCKET'
])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# configuration
SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']
SOURCE_PATH = f"s3://{SOURCE_BUCKET}/raw/"
TARGET_PATH = f"s3://{TARGET_BUCKET}/processed/"

def log_message(message: str) -> None:
    print(f"[{current_timestamp()}] {message}")

def define_source_schema() -> StructType:
    return StructType([
        StructField("CLAIMANT_ID", StringType(), True),        
        StructField("CLAIM_ITEM_ID", StringType(), True),      
        StructField("TYPE", StringType(), True),               
        StructField("RECEIVED_DATE", StringType(), True),      
        StructField("CHARGE_AMT", StringType(), True),         
        StructField("ALLOWED_AMT", StringType(), True),        
        StructField("DIAG_CODE_1", StringType(), True),        
        StructField("DIAG_CODE_2", StringType(), True),        
        StructField("DIAG_CODE_3", StringType(), True),        
        StructField("DIAG_CODE_4", StringType(), True),        
        StructField("DIAG_CODE_5", StringType(), True),        
        StructField("CLAIM_CODE", StringType(), True),         
        StructField("Rev Code/PROCEDURE_DESCRIPTION", StringType(), True), 
        StructField("CLAIM_CODE_MODIFIER", StringType(), True),     
        StructField("CLAIM_CODE_MODIFIER_2", StringType(), True),   
        StructField("UNITS", StringType(), True),              
        StructField("OI_IN_NETWORK", StringType(), True),      
        StructField("SERVICE_PROVIDER", StringType(), True),   
        StructField("SERVICE_ADDRESS_3", StringType(), True),  
        StructField("SERVICE_ADDRESS_2", StringType(), True),      
        
        StructField("COL_14", StringType(), True),             
        StructField("COL_15", StringType(), True),             
        StructField("COL_16", StringType(), True),             
        StructField("COL_17", StringType(), True),             
        StructField("COL_18", StringType(), True),             
        StructField("COL_19", StringType(), True),             
        StructField("COL_20", StringType(), True)              
    ])

def clean_and_standardize_data(df: DataFrame) -> DataFrame:
    """Clean and standardize the raw data for dimensional processing"""
    log_message("Starting data cleaning and standardization...")
    
    df_clean = df.withColumn("CHARGE_AMT", 
        regexp_replace(col("CHARGE_AMT"), "[$,]", "").cast(DoubleType())
    ).withColumn("ALLOWED_AMT", 
        regexp_replace(col("ALLOWED_AMT"), "[$,]", "").cast(DoubleType())
    ).withColumn("UNITS", 
        col("UNITS").cast(IntegerType())
    ).withColumn("RECEIVED_DATE", 
        to_date(col("RECEIVED_DATE"), "yyyy-MM-dd")
    )
    
    df_clean = df_clean \
        .withColumn("TYPE", 
            when(col("TYPE").isNull() | (trim(col("TYPE")) == ""), "Unknown")
            .otherwise(trim(upper(col("TYPE"))))
        ) \
        .withColumn("SERVICE_PROVIDER", 
            when(col("SERVICE_PROVIDER").isNull() | (trim(col("SERVICE_PROVIDER")) == ""), "Unknown")
            .otherwise(trim(upper(col("SERVICE_PROVIDER"))))
        ) \
        .withColumn("CLAIMANT_ID", 
            when(col("CLAIMANT_ID").isNull() | (trim(col("CLAIMANT_ID")) == ""), "Unknown")
            .otherwise(trim(col("CLAIMANT_ID")))
        ) \
        .withColumn("CLAIM_CODE", 
            when(col("CLAIM_CODE").isNull() | (trim(col("CLAIM_CODE")) == ""), "Unknown")
            .otherwise(trim(upper(col("CLAIM_CODE"))))
        ) \
        .withColumn("CLAIM_CODE_MODIFIER", 
            when(col("CLAIM_CODE_MODIFIER").isNull() | (trim(col("CLAIM_CODE_MODIFIER")) == ""), "Unknown")
            .otherwise(trim(upper(col("CLAIM_CODE_MODIFIER"))))
        ) \
        .withColumn("DIAG_CODE_1", 
            when(col("DIAG_CODE_1").isNull() | (trim(col("DIAG_CODE_1")) == ""), "Unknown")
            .otherwise(trim(upper(col("DIAG_CODE_1"))))
        )
    
    df_clean = df_clean \
        .withColumn("IS_VALID_AMOUNTS", 
            (col("CHARGE_AMT") >= 0) & (col("ALLOWED_AMT") >= 0)
        ) \
        .withColumn("IS_VALID_DATE", 
            col("RECEIVED_DATE").isNotNull() & (col("RECEIVED_DATE") <= current_date())
        ) \
        .withColumn("COST_SAVINGS", 
            col("CHARGE_AMT") - col("ALLOWED_AMT")
        ) \
        .withColumn("PROCESSING_TIMESTAMP", current_timestamp()) \
        .withColumn("SOURCE_FILE", input_file_name()) \
        .withColumn("year", year("RECEIVED_DATE")) \
        .withColumn("month", month("RECEIVED_DATE"))
    
    df_clean = df_clean.filter(
        col("CLAIMANT_ID").isNotNull() & 
        (trim(col("CLAIMANT_ID")) != "") &
        col("IS_VALID_AMOUNTS") &
        col("IS_VALID_DATE")
    )
    
    log_message(f"Records after cleaning: {df_clean.count()}")
    return df_clean

def create_business_key(df: DataFrame) -> DataFrame:
    """Create business key for deduplication - defines uniqueness"""
    return df.withColumn("business_key", 
        concat_ws("|", 
            col("CLAIMANT_ID"),
            col("RECEIVED_DATE"),
            col("SERVICE_PROVIDER"),
            col("CLAIM_CODE")
        )
    )

def get_existing_partitions(target_path: str) -> set:
    """Get list of existing partitions in target location"""
    try:
        s3 = boto3.client('s3')
        bucket = TARGET_BUCKET
        prefix = "processed/medical_claims_incremental/"
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        partitions = set()
        
        for prefix_info in response.get('CommonPrefixes', []):
            parts = prefix_info['Prefix'].split('/')
            for part in parts:
                if part.startswith('year='):
                    year = part.split('=')[1]
                    for part2 in parts:
                        if part2.startswith('month='):
                            month = part2.split('=')[1]
                            partitions.add((int(year), int(month)))
                            break
        
        log_message(f"Found existing partitions: {partitions}")
        return partitions
        
    except Exception as e:
        log_message(f"No existing partitions found or error: {e}")
        return set()

def process_partition_incrementally(df_new: DataFrame, year_val: int, month_val: int) -> DataFrame:
    """Process a single partition with incremental logic"""
    partition_path = f"{TARGET_PATH}medical_claims_incremental/year={year_val}/month={month_val}/"
    
    df_partition_new = df_new.filter(
        (col("year") == year_val) & (col("month") == month_val)
    )
    
    if df_partition_new.count() == 0:
        log_message(f"No new data for partition year={year_val}/month={month_val}")
        return spark.createDataFrame([], df_new.schema)
    
    log_message(f"Processing partition year={year_val}/month={month_val} with {df_partition_new.count()} new records")
    
    try:
        df_existing = spark.read.parquet(partition_path)
        log_message(f"Found {df_existing.count()} existing records in partition")
        
        df_combined = df_existing.union(df_partition_new)
        
        window_spec = Window.partitionBy("business_key").orderBy(desc("PROCESSING_TIMESTAMP"))
        df_deduplicated = df_combined \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        log_message(f"After deduplication: {df_deduplicated.count()} records")
        
    except Exception as e:
        log_message(f"No existing data found for partition (expected for new partitions): {e}")
        df_deduplicated = df_partition_new
    
    return df_deduplicated

def write_incremental_data(df: DataFrame) -> dict:
    """Write data incrementally by partition with upsert logic"""
    log_message("Starting incremental data write...")
    
    df_with_key = create_business_key(df)
    
    new_partitions = df_with_key.select("year", "month").distinct().collect()
    log_message(f"New data spans {len(new_partitions)} partitions: {[(p.year, p.month) for p in new_partitions]}")
    
    existing_partitions = get_existing_partitions(TARGET_PATH)
    
    processing_stats = {
        'partitions_processed': 0,
        'new_partitions': 0,
        'updated_partitions': 0,
        'total_records_written': 0,
        'duplicates_resolved': 0
    }
    
    for partition in new_partitions:
        year_val = partition.year
        month_val = partition.month
        
        df_final = process_partition_incrementally(df_with_key, year_val, month_val)
        
        if df_final.count() > 0:
            df_final.write \
                .mode('overwrite') \
                .parquet(f"{TARGET_PATH}medical_claims_incremental/year={year_val}/month={month_val}/")
            
            processing_stats['partitions_processed'] += 1
            processing_stats['total_records_written'] += df_final.count()
            
            if (year_val, month_val) in existing_partitions:
                processing_stats['updated_partitions'] += 1
                log_message(f"Updated existing partition year={year_val}/month={month_val}")
            else:
                processing_stats['new_partitions'] += 1
                log_message(f"Created new partition year={year_val}/month={month_val}")
    
    log_message(f"Incremental processing completed: {processing_stats}")
    return processing_stats

def perform_data_quality_checks(df: DataFrame) -> dict:
    """Perform data quality checks for incremental processing"""
    log_message("Performing incremental data quality checks...")
    
    total_records = df.count()
    
    unique_business_keys = df.select("business_key").distinct().count()
    duplicates = total_records - unique_business_keys
    
    comma_providers = df.filter(col("SERVICE_PROVIDER").contains(",")).count()
    
    fact_stats = df.select(
        min("CHARGE_AMT").alias("min_charge"),
        max("CHARGE_AMT").alias("max_charge"),
        avg("CHARGE_AMT").alias("avg_charge"),
        sum("COST_SAVINGS").alias("total_savings")
    ).collect()[0]
    
    date_stats = df.select(
        min("RECEIVED_DATE").alias("min_date"),
        max("RECEIVED_DATE").alias("max_date"),
        countDistinct("year", "month").alias("unique_months")
    ).collect()[0]
    
    quality_metrics = {
        'total_records': total_records,
        'unique_business_keys': unique_business_keys,
        'duplicates_found': duplicates,
        'comma_parsed_providers': comma_providers,
        'fact_statistics': {
            'min_charge': fact_stats['min_charge'],
            'max_charge': fact_stats['max_charge'],
            'avg_charge': fact_stats['avg_charge'],
            'total_savings': fact_stats['total_savings']
        },
        'date_coverage': {
            'min_date': str(date_stats['min_date']),
            'max_date': str(date_stats['max_date']),
            'months_spanned': date_stats['unique_months']
        }
    }
    
    log_message(f"Incremental data quality metrics: {quality_metrics}")
    return quality_metrics

def validate_incremental_results(stats: dict) -> bool:
    """Validate that incremental processing worked correctly"""
    log_message("Validating incremental processing results...")
    
    if stats['total_records_written'] == 0:
        log_message("ERROR: No records were written!")
        return False
    
    if stats['partitions_processed'] == 0:
        log_message("ERROR: No partitions were processed!")
        return False
    
    log_message(f"Incremental validation passed:")
    log_message(f"  - Records written: {stats['total_records_written']}")
    log_message(f"  - Partitions processed: {stats['partitions_processed']}")
    log_message(f"  - New partitions: {stats['new_partitions']}")
    log_message(f"  - Updated partitions: {stats['updated_partitions']}")
    
    return True

def main():
    """Main incremental processing function"""
    try:
        log_message(f"Starting INCREMENTAL medical claims processing: {args['JOB_NAME']}")
        log_message(f"Source: {SOURCE_PATH}")
        log_message(f"Target: {TARGET_PATH}")
        
        schema = define_source_schema()
        log_message("Reading raw CSV data...")
        
        df_raw = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'false') \
            .option('quote', '"') \
            .option('escape', '"') \
            .option('multiline', 'true') \
            .schema(schema) \
            .csv(f"{SOURCE_PATH}*.csv")
        
        initial_count = df_raw.count()
        log_message(f"Raw records loaded: {initial_count}")
        
        if initial_count == 0:
            log_message("⚠️ No new data to process")
            return
        
        df_clean = clean_and_standardize_data(df_raw)
        
        df_with_key = create_business_key(df_clean)
        
        quality_metrics = perform_data_quality_checks(df_with_key)
        
        processing_stats = write_incremental_data(df_clean)
        
        if not validate_incremental_results(processing_stats):
            raise Exception("Incremental processing validation failed")
        
        # Final summary
        log_message(f"INCREMENTAL processing completed successfully!")
        log_message(f"Input records: {initial_count}")
        log_message(f"Clean records: {df_clean.count()}")
        log_message(f"Final records written: {processing_stats['total_records_written']}")
        log_message(f"Partitions processed: {processing_stats['partitions_processed']}")
        log_message(f"New partitions: {processing_stats['new_partitions']}")
        log_message(f"Updated partitions: {processing_stats['updated_partitions']}")
        log_message(f"Comma-parsed providers: {quality_metrics['comma_parsed_providers']}")
        
        # Show sample results
        log_message("Sample processed data:")
        final_sample = spark.read.parquet(f"{TARGET_PATH}medical_claims_incremental/")
        final_sample.select(
            "CLAIMANT_ID", 
            "SERVICE_PROVIDER", 
            "TYPE", 
            "CHARGE_AMT", 
            "COST_SAVINGS",
            "PROCESSING_TIMESTAMP"
        ).show(10, truncate=False)
        
    except Exception as e:
        log_message(f"Incremental job failed with error: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()