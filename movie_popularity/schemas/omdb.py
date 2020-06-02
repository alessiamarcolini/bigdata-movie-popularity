import pyspark.sql.types as t

from utils import to_snake_case


def construct_omdb_schema(requested_flat_fields, requested_nested_fields):
    schema = []
    for key in requested_flat_fields:
        schema.append(t.StructField(key, t.StringType(), True))
    for key, values in requested_nested_fields.items():
        for value in values:
            schema.append(
                t.StructField(f"{key}_{to_snake_case(value)}", t.StringType(), True)
            )

    return t.StructType(schema)


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

schema_omdb_data = construct_omdb_schema(requested_flat_fields, requested_nested_fields)

schema_actors = t.StructType(
    [
        t.StructField("actor_id_0", t.IntegerType(), True),
        t.StructField("actor_id_1", t.IntegerType(), True),
        t.StructField("actor_id_2", t.IntegerType(), True),
        t.StructField("actor_id_3", t.IntegerType(), True),
    ]
)
