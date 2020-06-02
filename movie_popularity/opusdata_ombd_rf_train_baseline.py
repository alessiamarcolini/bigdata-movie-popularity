import os
from secrets import HADOOP_NAMENODE, HADOOP_USER_NAME, SPARK_URI

import pyspark.sql.functions as F
from hdfs import InsecureClient
from pyspark import SparkConf, SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    IndexToString,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = HADOOP_USER_NAME


client_hdfs = InsecureClient(f"http://{HADOOP_NAMENODE}:50070", user=HADOOP_USER_NAME)

# get preprocessed opusdata filename
hdfs_path = "/processed/opusdata_omdb.csv"

filename = [f for f in client_hdfs.list(hdfs_path) if f.endswith(".csv")][0]


conf = SparkConf().set(
    "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1"
)
sc = SparkContext(SPARK_URI, conf=conf)
sparkSession = (
    SparkSession.builder.config(
        "spark.mongodb.output.uri", "mongodb://127.0.0.1/bigdata.movie_popularity"
    )
    .appName("RandomForestClassifier")
    .getOrCreate()
)


data = sparkSession.read.csv(
    f"hdfs://{HADOOP_NAMENODE}:8020{hdfs_path}/{filename}",
    header=True,
    inferSchema=True,
)


columns_to_index = ["rating", "genre", "country"]


for col in columns_to_index:
    labelIndexer = StringIndexer(inputCol=col, outputCol=f"{col}_indexed").fit(data)
    data = labelIndexer.transform(data)

feature_cols = [
    "sequel",
    "runtime",
    "imdb_votes",
    "ratings_internet_movie_database",
    "ratings_rotten_tomatoes",
    "nominations",
    "wins",
    "won_golden_globes",
    "nominated_golden_globes",
    "won_oscars",
    "nominated_oscars",
    "won_baftas",
    "nominated_baftas",
    "actor_id_0",
    "actor_id_1",
    "actor_id_2",
    "actor_id_3",
    "rating_indexed",
    "genre_indexed",
    "country_indexed",
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
assembled_df = assembler.setHandleInvalid("skip").transform(data)


# Split the data into training and test sets (30% held out for testing)
(training_data, test_data) = assembled_df.randomSplit([0.7, 0.3], seed=1234)

num_folds = 5


evaluator = MulticlassClassificationEvaluator(
    labelCol="success", predictionCol="prediction", metricName="accuracy"
)


# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="success", featuresCol="features", numTrees=500)


paramGrid = (
    ParamGridBuilder().addGrid(param=rf.numTrees, values=[100, 300, 500]).build()
)

crossval = CrossValidator(
    estimator=rf,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=num_folds,
    seed=1234,
)


model = crossval.fit(training_data)


predictions_train = model.transform(training_data)
predictions_test = model.transform(test_data)

predictions_train.select("movie_name", "imdb_id", "prediction", "probability").show(5)

accuracy = evaluator.evaluate(predictions_train)
print("Train Accuracy = %g" % (accuracy))
accuracy = evaluator.evaluate(predictions_test)
print("Test Accuracy = %g" % (accuracy))


best_model = model.bestModel

feat_importances = list(
    ((col, imp) for col, imp in zip(feature_cols, best_model.featureImportances.values))
)


feat_importances.sort(key=lambda x: x[1], reverse=True)


print(feat_importances)


predictions_train_clean = predictions_train.select(
    "_id", "movie_name", "imdb_id", predictions_train["prediction"].cast("integer")
)
predictions_test_clean = predictions_test.select(
    "_id", "movie_name", "imdb_id", predictions_test["prediction"].cast("integer")
)

predictions_clean = predictions_train_clean.unionByName(predictions_test_clean)


predictions_clean = predictions_clean.withColumn("updated_on", F.current_date())


predictions_clean.write.format("mongo").mode("append").save()
