"""
AWS Glue ETL Job: Customer Trusted to Curated Zone
Purpose: Create curated customer dataset containing only customers with accelerometer data
Input: s3://stedi-s3/customer/trusted/, s3://stedi-s3/accelerometer/landing/
Output: s3://stedi-s3/customer/curated/
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

# Read customer trusted data
customer_trusted_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/customer/trusted"],
        "recurse": True
    },
    transformation_ctx="customer_trusted_df",
)

# Read accelerometer landing data
accelerometer_landing_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_df",
)

# Join customers with accelerometer data
# This ensures we only keep customers who have accelerometer readings
customer_accelerometer_join = Join.apply(
    frame1=customer_trusted_df,
    frame2=accelerometer_landing_df,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_accelerometer_join",
)

# Select only customer fields (drop accelerometer fields)
customer_curated_df = ApplyMapping.apply(
    frame=customer_accelerometer_join,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="customer_curated_df",
)

# Write curated customer data to S3
glue_context.write_dynamic_frame.from_options(
    frame=customer_curated_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="customer_curated_output",
)

job.commit()