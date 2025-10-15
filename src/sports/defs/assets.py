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


@dg.asset
def tennis_players_dataset(duckdb: DuckDBResource) -> None:
    base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
    csv_file = f"{base}/atp_players.csv"

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE players AS
            SELECT * REPLACE(
                CASE
                    WHEN dob IS NULL THEN NULL
                    WHEN SUBSTRING(CAST(dob AS VARCHAR), 5, 4) = '0000' THEN
                        CAST(strptime(
                            CONCAT(SUBSTRING(CAST(dob AS VARCHAR), 1, 4), '0101'),
                            '%Y%m%d'
                        ) AS date)
                    ELSE
                        CAST(strptime(dob, '%Y%m%d') AS date)
                END AS dob
            )
            FROM read_csv_auto(?, types={
                'dob': 'VARCHAR'
            });
        """, [csv_file])


@dg.asset(deps=[tennis_players_dataset])
def tennis_players_name_dataset(duckdb: DuckDBResource) -> None:
    concatenate_query = """
        ALTER TABLE players ADD COLUMN IF NOT EXISTS name_full VARCHAR;
        UPDATE players
        SET name_full = name_first || ' ' || name_last;
    """

    with duckdb.get_connection() as conn:
        conn.execute(concatenate_query)
