import streamlit as st
import duckdb
from streamlit_searchbox import st_searchbox
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide") 

atp_duck = duckdb.connect('tennis.duckdb', read_only=True)

def search_players(search_term):
    query = '''
    SELECT DISTINCT winner_name AS player
    FROM matches
    WHERE player ilike '%' || $1 || '%'
    UNION
    SELECT DISTINCT loser_name AS player
    FROM matches
    WHERE player ilike '%' || $1 || '%'
    '''
    values = atp_duck.execute(query, parameters=[search_term]).fetchall()
    return [value[0] for value in values]

def add_empty_column_if_needed(df, player1, player1_wins, player2, player2_wins):
    if player1_wins == 0:
        df[player1] = 0
    if player2_wins == 0:
        df[player2] = 0

st.title("Tennis Players Head to Head")
