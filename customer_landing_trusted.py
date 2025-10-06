"""
AWS Glue ETL Job: Customer Landing to Trusted Zone
Purpose: Filter customer data to include only those who agreed to share data for research
Input: s3://stedi-s3/customer/landing/
Output: s3://stedi-s3/customer/trusted/
"""

import sys
from awsglue.transforms import Filter
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

# Read customer landing data from S3
customer_landing_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_landing_df",
)

# Filter customers who agreed to share data for research
# shareWithResearchAsOfDate != 0 indicates consent
customers_with_consent = Filter.apply(
    frame=customer_landing_df,
    f=lambda row: (row["shareWithResearchAsOfDate"] != 0),
    transformation_ctx="customers_with_consent",
)

# Write filtered customer data to trusted zone
glue_context.write_dynamic_frame.from_options(
    frame=customers_with_consent,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="customer_trusted_output",
)

job.commit()