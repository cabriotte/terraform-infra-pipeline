import sys
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(F, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://b3-tech-challenge-bucket/raw/pregao/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node",
)

Mapped_node = ApplyMapping.apply(
    frame=AmazonS3_node,
    mappings=[
        ("Código", "string", "Código", "string"),
        ("Ação", "string", "Ação", "string"),
        ("Tipo", "string", "Tipo", "string"),
        ("Qtde Teórica", "string", "Qtde Teórica", "double"),
        ("data_extracao", "long", "data_extracao", "date"),
    ],
    transformation_ctx="Mapped_node",
)

Aggregated_node = sparkAggregate(
    glueContext,
    parentFrame=Mapped_node,
    groups=["Ação", "data_extracao"],
    aggs=[["Qtde Teórica", "sum"]],
    transformation_ctx="Aggregated_node",
)

df = Aggregated_node.toDF()

df = df.withColumn(
    "happened_within_last_week",
    F.col("data_extracao") >= F.date_sub(F.current_date(), 7),
)

df = (
    df.withColumnRenamed("Ação", "ticker")
    .withColumnRenamed("data_extracao", "extraction_date")
    .withColumnRenamed("sum(Qtde Teórica)", "theoretical_quantity")
)

Final_node = DynamicFrame.fromDF(df, glueContext, "Final_node")

EvaluateDataQuality().process_rows(
    frame=Final_node,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

sink = glueContext.getSink(
    path="s3://b3-tech-challenge-bucket/refined/pregao/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["extraction_date", "ticker"],
    enableUpdateCatalog=True,
    transformation_ctx="S3Sink",
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(Final_node)

job.commit()
