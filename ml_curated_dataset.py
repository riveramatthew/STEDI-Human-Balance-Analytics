"""
AWS Glue ETL Job: Machine Learning Curated Dataset
Purpose: Join step trainer and accelerometer data for ML model training
Input: s3://stedi-s3/step_trainer/trusted/, s3://stedi-s3/accelerometer/trusted/
Output: s3://stedi-s3/ML_curated/
"""

import sys
from awsglue.transforms import Join
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

# Read accelerometer trusted data
accelerometer_trusted_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_df",
)

# Read step trainer trusted data
step_trainer_trusted_df = glue_context.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_df",
)

# Join step trainer and accelerometer data on timestamp
# This combines sensor readings with accelerometer data at matching times
ml_curated_df = Join.apply(
    frame1=step_trainer_trusted_df,
    frame2=accelerometer_trusted_df,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="ml_curated_df",
)

# Write machine learning curated dataset to S3
# This dataset contains all fields from both sources for ML training
glue_context.write_dynamic_frame.from_options(
    frame=ml_curated_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/ML_curated/",
        "partitionKeys": []
    },
    transformation_ctx="ml_curated_output",
)

job.commit()