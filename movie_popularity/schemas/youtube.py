import pyspark.sql.types as t

schema_stats = t.StructType(
    [
        t.StructField("youtube_view_count", t.StringType(), True),
        t.StructField("youtube_engagement_score", t.StringType(), True),
        t.StructField("youtube_positive_engagement_score", t.StringType(), True),
    ]
)
