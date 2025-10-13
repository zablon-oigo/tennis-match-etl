import dagster as dg
from dagster_duckdb import DuckDBResource
import pandas as pd

@dg.asset
def tennis_matches_dataset(duckdb: DuckDBResource) -> None:
    base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"

    csv_files = [
        f"{base}/atp_matches_{year}.csv"
        for year in range(2000, 2024)
    ]
    create_query = """
        CREATE OR REPLACE TABLE matches AS
        SELECT * REPLACE(
            cast(strptime(tourney_date, '%Y%m%d') AS date) AS tourney_date
        )
        FROM read_csv_auto(?, union_by_name=True, types={
            'winner_seed': 'VARCHAR',
            'loser_seed': 'VARCHAR',
            'tourney_date': 'VARCHAR'
        });
    """
    with duckdb.get_connection() as conn:
        conn.execute(create_query, [csv_files])
import dagster as dg
from dagster_duckdb import DuckDBResource


