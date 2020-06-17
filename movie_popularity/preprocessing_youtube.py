import os
from secrets import HADOOP_NAMENODE, HADOOP_USER_NAME, SPARK_URI

import google_auth_oauthlib.flow
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as t
from googleapiclient.errors import HttpError
from hdfs import InsecureClient
from pyspark import SparkContext
from pyspark.sql import SparkSession

import youtube_utils
from schemas.youtube import schema_stats

os.environ["HADOOP_USER_NAME"] = HADOOP_USER_NAME


client_hdfs = InsecureClient(f"http://{HADOOP_NAMENODE}:50070", user=HADOOP_USER_NAME)


# get preprocessed opusdata filename
hdfs_path = "/processed/opusdata_omdb.csv"

filename = [f for f in client_hdfs.list(hdfs_path) if f.endswith(".csv")][0]


sc = SparkContext(SPARK_URI)

parent_dir = os.path.dirname(os.path.abspath(__file__))
sc.addPyFile(os.path.join(parent_dir, "youtube_utils.py"))
sc.addPyFile(os.path.join(parent_dir, "secrets.py"))
# sc.addPyFile('/home/utente/bigdata-movie-popularity/movie_popularity/youtube_utils.py')

sparkSession = (
    SparkSession.builder.appName("example-pyspark-read-and-write")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)


# Read from hdfs
opusdata_omdb = sparkSession.read.csv(
    f"hdfs://{HADOOP_NAMENODE}:8020{hdfs_path}/{filename}",
    header=True,
    inferSchema=True,
)


opusdata_omdb.show()


scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_service_name = "youtube"
api_version = "v3"


@F.udf(returnType=t.StringType())
def id_from_title(movie_name):
    youtube = youtube_utils.get_authenticated_service(
        api_service_name, api_version, scopes, n_tries=0
    )

    n_tries = 0
    success = False
    while not success and n_tries < 19:
        try:
            request = youtube.search().list(
                part="snippet", q=f"{movie_name} official trailer"
            )
            response = request.execute()
            success = True

        except HttpError as e:
            n_tries += 1
            youtube = youtube_utils.get_authenticated_service(
                api_service_name, api_version, scopes, n_tries=n_tries,
            )
    if not success:
        return None

    available_videos = response["items"]

    for video in available_videos:
        try:
            kind = video["id"]["kind"]
            if kind == "youtube#video":
                video_id = video["id"]["videoId"]
                return video_id

        except KeyError as e:
            return None
    return None


opusdata_youtube = opusdata_omdb.withColumn(
    "youtube_video_id", id_from_title("movie_name")
)

opusdata_youtube.show()


@F.udf(returnType=schema_stats)
def stats_from_id(video_id):
    if not video_id:
        return None, None, None

    youtube = youtube_utils.get_authenticated_service(
        api_service_name, api_version, scopes, n_tries=0
    )

    n_tries = 0
    success = False
    while not success and n_tries < 19:
        try:
            request = youtube.videos().list(part="statistics", id=video_id)
            response = request.execute()
            success = True

        except HttpError as e:
            n_tries += 1
            youtube = youtube_utils.get_authenticated_service(
                api_service_name, api_version, scopes, n_tries=n_tries,
            )
    if not success:
        return None, None, None

    try:
        stats = response["items"][0]["statistics"]

        view_count = int(stats["viewCount"])
        like_count = int(stats["likeCount"])
        dislike_count = int(stats["dislikeCount"])

        engagement_score = (like_count + dislike_count) / view_count
        positive_engagement_score = like_count / dislike_count

    except KeyError as e:
        return None, None, None

    return t.Row(
        "youtube_view_count",
        "youtube_engagement_score",
        "youtube_positive_engagement_score",
    )(view_count, engagement_score, positive_engagement_score)


opusdata_youtube.show()


opusdata_youtube_stats = opusdata_youtube.withColumn(
    "youtube_stats", F.explode(F.array(stats_from_id("youtube_video_id")))
)

fields_name = [
    field.name
    for field in opusdata_youtube_stats.schema.fields
    if field.name != "youtube_stats"
]
opusdata_youtube_stats = opusdata_youtube_stats.select(*fields_name, "youtube_stats.*")

# TODO: add upload date feature

opusdata_youtube_stats.repartition(1).write.mode("overwrite").option(
    "header", True
).csv(f"hdfs://{HADOOP_NAMENODE}:8020/processed/opusdata_omdb_youtube.csv")
