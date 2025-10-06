"""
AWS Glue ETL Job: Accelerometer Landing to Trusted Zone
Purpose: Join accelerometer data with trusted customers to ensure privacy compliance
Input: s3://stedi-s3/accelerometer/landing/, s3://stedi-s3/customer/trusted/
Output: s3://stedi-s3/accelerometer/trusted/
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

# Read customer trusted data (customers who consented to share data)
customer_trusted_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="customer_trusted_df",
)

# Join accelerometer data with trusted customers
# Only keep accelerometer records for customers who gave consent
accelerometer_customer_join = Join.apply(
    frame1=accelerometer_landing_df,
    frame2=customer_trusted_df,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="accelerometer_customer_join",
)

# Select only accelerometer fields (drop customer fields)
accelerometer_trusted_df = ApplyMapping.apply(
    frame=accelerometer_customer_join,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "float"),
        ("y", "double", "y", "float"),
        ("z", "double", "z", "float"),
    ],
    transformation_ctx="accelerometer_trusted_df",
)

# Write accelerometer trusted data to S3
glue_context.write_dynamic_frame.from_options(
    frame=accelerometer_trusted_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_output",
)

job.commit()