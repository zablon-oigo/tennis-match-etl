import streamlit as st
import duckdb
from streamlit_searchbox import st_searchbox
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide") 

atp_duck = duckdb.connect('tennis.duckdb', read_only=True)

















