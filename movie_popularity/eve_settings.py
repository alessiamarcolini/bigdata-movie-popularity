movie_popularity = {
    "item_tile": "popularity",
    "schema": {
        "movie_name": {"type": "string"},
        "imdb_id": {"type": "string"},
        "prediction": {"type": "int"},
        "updated_on": {"type": "datetime"},
    },
    "additional_lookup": {"url": 'regex("tt[\w]+")', "field": "imdb_id",},
    "datasource": {"default_sort": [("updated_on", -1)]},
}

DOMAIN = {"movie_popularity": movie_popularity}

MONGO_HOST = "localhost"
MONGO_PORT = 27017

MONGO_DBNAME = "bigdata"
