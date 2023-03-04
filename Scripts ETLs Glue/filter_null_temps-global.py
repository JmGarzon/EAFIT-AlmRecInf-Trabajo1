import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node temp_global
temp_global_node1677645041047 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://climate-change-datalake/raw/earth-temp/global/"],
        "recurse": True,
    },
    transformation_ctx="temp_global_node1677645041047",
)

# Script generated for node Custom Transform
SqlQuery282 = """
select * from myDataSource
where LandAverageTemperature != ""
"""
CustomTransform_node1677647017495 = sparkSqlQuery(
    glueContext,
    query=SqlQuery282,
    mapping={"myDataSource": temp_global_node1677645041047},
    transformation_ctx="CustomTransform_node1677647017495",
)

# Script generated for node Amazon S3
AmazonS3_node1677648981081 = glueContext.write_dynamic_frame.from_options(
    frame=CustomTransform_node1677647017495,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://climate-change-datalake/trusted/earth-temp/global/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1677648981081",
)

job.commit()
