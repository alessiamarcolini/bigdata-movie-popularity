from secrets import OMDB_API_KEY, OMDB_API_KEY_fallback

import pyspark.sql.functions as F
import pyspark.sql.types as t
from omdb import OMDBClient
from requests.exceptions import HTTPError

from schemas.omdb import (
    requested_flat_fields,
    requested_nested_fields,
    schema_omdb_data,
)
from utils import to_snake_case

# ----------------------
# PREPROCESSING OPUSDATA
# ----------------------


@F.udf(returnType=t.IntegerType())
def success(arguments):
    total_box_office, production_budget = arguments

    profit = (0.5 * total_box_office) - production_budget
    profit_censored = 1 if profit > 0 else 0
    return profit_censored


# ---------------------------
# PREPROCESSING OPUSDATA_OMDB
# ---------------------------


@F.udf(returnType=schema_omdb_data)
def omdb_data(arguments):
    movie_name, year = arguments
    client = OMDBClient(apikey=OMDB_API_KEY)
    try:
        result = client.get(title=movie_name, year=year, fullplot=True, tomatoes=True)
    except HTTPError as e:
        print(e)

        client.set_default("apikey", OMDB_API_KEY_fallback)

        result = client.get(title=movie_name, year=year, fullplot=True, tomatoes=True)

    result_to_keep = {}

    for key in requested_flat_fields:
        result_to_keep[key] = result.get(key, None)

    for nested_field in requested_nested_fields:
        requested_nested_list = requested_nested_fields[nested_field]
        nested_list = result.get(nested_field, None)

        if nested_list:
            for nested_dict in nested_list:
                source = nested_dict.get("source", None)

                if source:
                    value = nested_dict.get("value", None)

                    if source in requested_nested_list:

                        source_formatted = to_snake_case(source)
                        key = f"{nested_field}_{source_formatted}"

                        result_to_keep[key] = value

            requested_sources = requested_nested_fields[nested_field]
            for requested_source in requested_sources:
                source_formatted = to_snake_case(requested_source)
                key = f"{nested_field}_{source_formatted}"
                if not key in result_to_keep:
                    result_to_keep[key] = None

        else:
            requested_sources = requested_nested_fields[nested_field]
            for requested_source in requested_sources:
                source_formatted = to_snake_case(requested_source)
                key = f"{nested_field}_{source_formatted}"
                result_to_keep[key] = None

    return t.Row(*list(result_to_keep.keys()))(*list(result_to_keep.values()))


@F.udf(returnType=t.IntegerType())
def general_awards_by_keyword(awards_str, keyword):
    n_nominations = awards_str.split(keyword)[0].split()[-1]
    try:
        n_nominations_int = int(n_nominations)
    except ValueError as e:
        n_nominations_int = 0
    return n_nominations_int


@F.udf(returnType=t.IntegerType())
def won_by_keyword(awards_str, award_name):
    awards_str = awards_str.lower()

    try:
        won_or_nominated = awards_str.split(award_name)[0].split()[-2]
        if won_or_nominated == "won":
            n_won = int(awards_str.split(award_name)[0].split()[-1])
        else:
            n_won = 0
    except IndexError as e:
        n_won = 0

    return n_won


@F.udf(returnType=t.IntegerType())
def nominated_by_keyword(awards_str, award_name):
    awards_str = awards_str.lower()

    try:
        won_or_nominated = awards_str.split(award_name)[0].split()[-2]
        if won_or_nominated == "for":
            n_nominated = int(awards_str.split(award_name)[0].split()[-1])
        else:
            n_nominated = 0
    except IndexError as e:
        n_nominated = 0

    return n_nominated
