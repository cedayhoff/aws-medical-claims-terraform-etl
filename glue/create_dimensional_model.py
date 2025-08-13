#!/usr/bin/env python3

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import date

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'SOURCE_BUCKET', 
    'TARGET_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']
PROCESSED_PATH = f"s3://{SOURCE_BUCKET}/processed/medical_claims_incremental/"
CURATED_PATH = f"s3://{TARGET_BUCKET}/curated/"

def log_message(message: str) -> None:
    print(f"[{message}]")

def create_simple_date_dimension() -> DataFrame:
    log_message("Creating simple date dimension...")
    
    start_date = "2020-01-01"
    end_date = "2025-12-31"
    
    date_df = spark.sql(f"""
        SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_array
    """).select(explode(col("date_array")).alias("full_date"))
    
    date_dim = date_df.select(
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        quarter("full_date").alias("quarter")
    )
    
    log_message(f"Date dimension created with {date_dim.count()} records")
    return date_dim

def create_simple_dimensions_and_facts() -> None:
    log_message("Creating dimensional model...")
    
    processed_df = spark.read.parquet(PROCESSED_PATH)
    log_message(f"Read {processed_df.count()} processed records")
    
    processed_df_deduped = processed_df.dropDuplicates(["CLAIM_ITEM_ID"]).cache()
    deduped_count = processed_df_deduped.count()
    log_message(f"After deduplication: {deduped_count} unique claims")
    
    claimant_dim = processed_df_deduped.select("CLAIMANT_ID").distinct() \
        .withColumn("claimant_key", row_number().over(Window.orderBy("CLAIMANT_ID")) + 1000) \
        .select("claimant_key", col("CLAIMANT_ID").alias("claimant_id"))
    
    log_message(f"Claimant dimension: {claimant_dim.count()} records")
    
    provider_dim = processed_df_deduped.select("SERVICE_PROVIDER").distinct() \
        .withColumn("provider_key", row_number().over(Window.orderBy("SERVICE_PROVIDER")) + 2000) \
        .select("provider_key", col("SERVICE_PROVIDER").alias("provider_name"))
    
    log_message(f"Provider dimension: {provider_dim.count()} records")
    
    date_dim = create_simple_date_dimension()
    
    fact_table = processed_df_deduped \
        .join(broadcast(claimant_dim), processed_df_deduped.CLAIMANT_ID == claimant_dim.claimant_id, "left") \
        .join(broadcast(provider_dim), processed_df_deduped.SERVICE_PROVIDER == provider_dim.provider_name, "left") \
        .withColumn("received_date_key", date_format("RECEIVED_DATE", "yyyyMMdd").cast("int")) \
        .select(
            monotonically_increasing_id().alias("claim_key"),
            col("CLAIM_ITEM_ID").alias("claim_id"),
            coalesce(col("claimant_key"), lit(0)).alias("claimant_key"),
            coalesce(col("provider_key"), lit(0)).alias("provider_key"),
            col("received_date_key").alias("received_date_key"),
            col("CHARGE_AMT").cast("double").alias("charge_amount"),
            col("ALLOWED_AMT").cast("double").alias("allowed_amount"),
            coalesce(col("UNITS").cast("int"), lit(1)).alias("units"),
            (col("CHARGE_AMT").cast("double") - col("ALLOWED_AMT").cast("double")).alias("cost_savings_amount"),
            when(col("CHARGE_AMT") > 0, 
                 col("ALLOWED_AMT").cast("double") / col("CHARGE_AMT").cast("double")
            ).otherwise(0.0).alias("reimbursement_rate"),
            when(upper(col("OI_IN_NETWORK")) == "Y", True).otherwise(False).alias("in_network"),
            coalesce(col("TYPE"), lit("Unknown")).alias("claim_type"),
            coalesce(col("CLAIM_CODE"), lit("Unknown")).alias("claim_code"),
            coalesce(col("Rev Code/PROCEDURE_DESCRIPTION"), lit("Unknown")).alias("procedure_description"),
            coalesce(col("CLAIM_CODE_MODIFIER"), lit("Unknown")).alias("claim_modifier_1"),
            coalesce(col("CLAIM_CODE_MODIFIER_2"), lit("Unknown")).alias("claim_modifier_2"),
            coalesce(col("DIAG_CODE_1"), lit("Unknown")).alias("diagnosis_code_1"),
            coalesce(col("DIAG_CODE_2"), lit("Unknown")).alias("diagnosis_code_2"),
            coalesce(col("DIAG_CODE_3"), lit("Unknown")).alias("diagnosis_code_3"),
            coalesce(col("DIAG_CODE_4"), lit("Unknown")).alias("diagnosis_code_4"),
            coalesce(col("DIAG_CODE_5"), lit("Unknown")).alias("diagnosis_code_5")
        )
    
    final_count = fact_table.count()
    log_message(f"Fact table: {final_count} records (should equal {deduped_count})")
    
    if final_count != deduped_count:
        log_message(f"ERROR: Record count mismatch! Expected {deduped_count}, got {final_count}")
    else:
        log_message("SUCCESS: Record counts match perfectly!")
    
    fact_final = fact_table
    
    log_message("Writing dimensional model...")
    date_dim.write.mode('overwrite').parquet(f"{CURATED_PATH}dim_date/")
    claimant_dim.write.mode('overwrite').parquet(f"{CURATED_PATH}dim_claimant/")
    provider_dim.write.mode('overwrite').parquet(f"{CURATED_PATH}dim_provider/")
    fact_final.write.mode('overwrite').parquet(f"{CURATED_PATH}fact_claims/")
    
    log_message("Parquet files written successfully")
    
    total_claims = fact_final.count()
    total_charge = fact_final.agg(sum("charge_amount")).collect()[0][0]
    total_allowed = fact_final.agg(sum("allowed_amount")).collect()[0][0]
    
    log_message(f"COMPLETED: {total_claims:,} claims, ${total_charge:,.2f} charged, ${total_allowed:,.2f} allowed")

def main():
    try:
        log_message(f"Starting simplified dimensional model: {args['JOB_NAME']}")
        create_simple_dimensions_and_facts()
        log_message("SUCCESS: Dimensional model creation completed!")
        
    except Exception as e:
        log_message(f"ERROR: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()