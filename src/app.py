# app.py

import pandas as pd
from dash import Dash, dcc, html

data = (
    # pd.read_csv("avocado.csv")
    # .query("type == 'conventional' and region == 'Albany'")
    # .assign(Date=lambda data: pd.to_datetime(data["Date"], format="%Y-%m-%d"))
    # .sort_values(by="Date")
)

app = Dash(__name__)