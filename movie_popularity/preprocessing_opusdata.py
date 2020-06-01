import os
from secrets import HADOOP_NAMENODE, HADOOP_USER_NAME, SPARK_URI

import pyspark
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession

from udfs import success

os.environ["HADOOP_USER_NAME"] = HADOOP_USER_NAME


sc = SparkContext(SPARK_URI)

sparkSession = (
    SparkSession.builder.appName("processing-opusdata")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)

# Read from hdfs
opusdata = sparkSession.read.csv(
    f"hdfs://{HADOOP_NAMENODE}:8020/raw/opusdata.csv", header=True, inferSchema=True
)

opusdata_filter_0 = opusdata.filter(opusdata["production_budget"] != 0)


opusdata_filter_0 = opusdata_filter_0.filter(
    opusdata_filter_0["domestic_box_office"] != 0
)

opusdata_filter_0 = opusdata_filter_0.filter(
    opusdata_filter_0["international_box_office"] != 0
)

opusdata_dropped = opusdata_filter_0.drop(
    "movie_odid", "running_time", "production_method", "creative_type", "source"
)

opusdata_years = opusdata_dropped.filter(opusdata_dropped["production_year"] >= 2010)

opusdata_distinct = opusdata_years.dropDuplicates(["movie_name", "production_year"])

opusdata_total_box_office = opusdata_distinct.withColumn(
    "total_box_office",
    opusdata_distinct["domestic_box_office"]
    + opusdata_distinct["international_box_office"],
).drop("domestic_box_office", "international_box_office")


opusdata_droppped_na = opusdata_total_box_office.na.drop(subset=["genre", "sequel"])


# ### Get "success" [1]
# [1] _Rhee, Travis Ginmu, and Farhana Zulkernine.
# "Predicting movie box office profitability: A neural network approach."
# 2016 15th IEEE International Conference on Machine Learning and Applications (ICMLA).
# IEEE, 2016._
#
# Profit = (1⁄2 * total_box_office) – production_budget


opusdata_success = opusdata_droppped_na.withColumn(
    "success", success(F.array("total_box_office", "production_budget"))
)

opusdata_success.repartition(1).write.mode("overwrite").option("header", True).csv(
    f"hdfs://{HADOOP_NAMENODE}:8020/processed/opusdata.csv"
)
