import os
from secrets import HADOOP_NAMENODE, HADOOP_USER_NAME, OMDB_API_KEY, SPARK_URI

import omdb
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as t
from hdfs import InsecureClient
from omdb import OMDBClient
from pyspark import SparkContext
from pyspark.sql import SparkSession
from requests.exceptions import HTTPError

from udfs import (
    encode_authors,
    general_awards_by_keyword,
    nominated_by_keyword,
    omdb_data,
    won_by_keyword,
)

os.environ["HADOOP_USER_NAME"] = HADOOP_USER_NAME


client_hdfs = InsecureClient(f"http://{HADOOP_NAMENODE}:50070", user=HADOOP_USER_NAME)


# get preprocessed opusdata filename
hdfs_path = "/processed/opusdata.csv"

filename = [f for f in client_hdfs.list(hdfs_path) if f.endswith(".csv")][0]


sc = SparkContext(SPARK_URI)
sparkSession = (
    SparkSession.builder.appName("preprocessing-opusdata-and-omdb")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)


# Read from hdfs
opusdata = sparkSession.read.csv(
    f"hdfs://{HADOOP_NAMENODE}:8020{hdfs_path}/{filename}",
    header=True,
    inferSchema=True,
)


opusdata.show()


omdb.set_default("apikey", OMDB_API_KEY)
client = OMDBClient(apikey=OMDB_API_KEY)

requested_flat_fields = [
    "runtime",
    "director",
    "actors",
    "country",
    "awards",
    "imdb_votes",
    "imdb_id",
]
requested_nested_fields = {
    "ratings": ["Internet Movie Database", "Rotten Tomatoes", "Metacritic"]
}


opusdata_omdb = opusdata.withColumn(
    "omdb_data", F.explode(F.array(omdb_data(F.array("movie_name", "production_year"))))
)
opusdata_fields_name = [field.name for field in opusdata.schema.fields]

opusdata_ombd = opusdata_omdb.select(*opusdata_fields_name, "omdb_data.*")


opusdata_ombd_id_not_null = opusdata_ombd.na.drop(
    subset=[
        "imdb_id",
        "ratings_internet_movie_database",
        "ratings_rotten_tomatoes",
        "ratings_metacritic",
    ]
)

opusdata_ombd_no_id_duplicated = opusdata_ombd_id_not_null.dropDuplicates(["imdb_id"])


# ### Processing awards

keywords_general = ["nomination", "win"]

awards_name = ["golden globe", "oscar", "bafta"]


opusdata_awards_categorized = opusdata_ombd_no_id_duplicated
for general_keyword in keywords_general:
    opusdata_awards_categorized = opusdata_awards_categorized.withColumn(
        f"{general_keyword}s",
        general_awards_by_keyword("awards", F.lit(general_keyword)),
    )


for award_name in awards_name:
    award_name_formatted = "_".join(award_name.split())
    opusdata_awards_categorized = opusdata_awards_categorized.withColumn(
        f"won_{award_name_formatted}s", won_by_keyword("awards", F.lit(award_name))
    )
    opusdata_awards_categorized = opusdata_awards_categorized.withColumn(
        f"nominated_{award_name_formatted}s",
        nominated_by_keyword("awards", F.lit(award_name)),
    )

opusdata_awards_categorized = opusdata_awards_categorized.drop("awards")


# ### Scale rankings [0..1]

# scale imdb ratings
opusdata_scaled_ratings = opusdata_awards_categorized.withColumn(
    "ratings_internet_movie_database",
    F.split(F.col("ratings_internet_movie_database"), "/").cast("array<float>"),
)
opusdata_scaled_ratings = opusdata_scaled_ratings.withColumn(
    "ratings_internet_movie_database", F.col("ratings_internet_movie_database")[0] / 10
)


# scale rotten tomatoes ratings
opusdata_scaled_ratings = opusdata_scaled_ratings.withColumn(
    "ratings_rotten_tomatoes",
    F.split(F.col("ratings_rotten_tomatoes"), "%").cast("array<int>"),
)
opusdata_scaled_ratings = opusdata_scaled_ratings.withColumn(
    "ratings_rotten_tomatoes", F.col("ratings_rotten_tomatoes")[0] / 100
)


# scale metacritic ratings
opusdata_scaled_ratings = opusdata_scaled_ratings.withColumn(
    "ratings_metacritic", F.split(F.col("ratings_metacritic"), "/").cast("array<int>")
)
opusdata_scaled_ratings = opusdata_scaled_ratings.withColumn(
    "ratings_metacritic", F.col("ratings_metacritic")[0] / 100
)


# remove comma from imdb_votes
opusdata_votes = opusdata_scaled_ratings.withColumn(
    "imdb_votes", F.regexp_replace("imdb_votes", ",", "")
)


# ### Encode actors
unique_actors = set()

for i, row in enumerate(opusdata_votes.rdd.collect()):
    actors = row["actors"]
    unique_actors.update([a.strip().lower() for a in actors.split(",")])


actors_id_dict = {actor: i for i, actor in enumerate(unique_actors)}


opusdata_actors = opusdata_votes.withColumn(
    "actors_ids", F.explode(F.array(encode_authors("actors")))
)

opusdata_fields_name = [
    field.name
    for field in opusdata_actors.schema.fields
    if field.name != "actors_ids" and field.name != "actors"
]
opusdata_actors = opusdata_actors.select(*opusdata_fields_name, "actors_ids.*")


# ### Runtime - remove "min"


opusdata_runtime = opusdata_actors.withColumn(
    "runtime", F.split(F.col("runtime"), " ").cast("array<string>")
)
opusdata_runtime = opusdata_runtime.withColumn("runtime", F.col("runtime")[0])


# ### Keep only first country

opusdata_first_country = opusdata_runtime.withColumn(
    "country", F.split(F.col("country"), ",").cast("array<string>")
)
opusdata_first_country = opusdata_first_country.withColumn(
    "country", F.col("country")[0]
)


# ### Keep only first director

opusdata_first_director = opusdata_first_country.withColumn(
    "director", F.split(F.col("director"), ",").cast("array<string>")
)
opusdata_first_director = opusdata_first_director.withColumn(
    "director", F.col("director")[0]
)

opusdata_first_director.repartition(1).write.mode("overwrite").option(
    "header", True
).csv(f"hdfs://{HADOOP_NAMENODE}:8020/processed/opusdata_omdb.csv")
