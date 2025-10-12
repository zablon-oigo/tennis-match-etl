from dagster_duckdb import DuckDBResource

import dagster as dg

database_resource = DuckDBResource(database="tennis.duckdb")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )