"""
AWS Glue ETL Job: Step Trainer Landing to Trusted Zone
Purpose: Join step trainer data with curated customers to ensure data quality
Input: s3://stedi-s3/step_trainer/landing/, s3://stedi-s3/customer/curated/
Output: s3://stedi-s3/step_trainer/trusted/
"""

import sys
from awsglue.transforms import Join, ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Read customer curated data (customers with accelerometer data)
customer_curated_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/customer/curated/"],
        "recurse": True
    },
    transformation_ctx="customer_curated_df",
)

# Read step trainer landing data
step_trainer_landing_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_df",
)

# Join step trainer data with curated customers by serial number
# This ensures we only keep step trainer data for valid customers
step_trainer_customer_join = Join.apply(
    frame1=step_trainer_landing_df,
    frame2=customer_curated_df,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="step_trainer_customer_join",
)

# Select only step trainer fields (drop customer fields)
step_trainer_trusted_df = ApplyMapping.apply(
    frame=step_trainer_customer_join,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="step_trainer_trusted_df",
)

# Write step trainer trusted data to S3
glue_context.write_dynamic_frame.from_options(
    frame=step_trainer_trusted_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_output",
)

job.commit()