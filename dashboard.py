import pandas as pd
import dash
from dash import dash_table, dcc, html, Input, Output
import plotly.express as px
from pathlib import Path

# --- Load master CSVs ---
outputs_dir = Path("outputs")
in_review_file = outputs_dir / "master_fmcsa_in_review.csv"
removed_file = outputs_dir / "master_fmcsa_removed.csv"

df_in = pd.read_csv(in_review_file, dtype=str)
df_rm = pd.read_csv(removed_file, dtype=str)

# Optional: sort by state and ZipCode
df_in = df_in.sort_values(["PhysicalState", "ZipCode"])
df_rm = df_rm.sort_values(["PhysicalState", "ZipCode"])

# --- Initialize Dash app ---
app = dash.Dash(__name__)
app.title = "FMCSA Provider Dashboard"

# --- App Layout ---
app.layout = html.Div([
    html.H1("FMCSA Provider Dashboard", style={"textAlign": "center"}),

    # Toggle between In Review / Removed
    html.Div([
        html.Label("Select Category:"),
        dcc.RadioItems(
            ["In Review", "Removed"],
            "In Review",
            id="category-toggle",
            inline=True
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # Scrollable/Searchable table
    dash_table.DataTable(
        id="provider-table",
        columns=[{"name": i, "id": i} for i in df_in.columns],
        data=df_in.to_dict("records"),
        page_size=20,
        filter_action="native",
        sort_action="native",
        style_table={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"},
        style_cell={"textAlign": "left", "minWidth": "100px", "width": "150px", "maxWidth": "300px"},
    ),

    html.Br(),

    # Map to filter by state
    dcc.Graph(id="us-map")
])


# --- Callbacks ---

# Update table when toggle changes or map click occurs
@app.callback(
    Output("provider-table", "data"),
    Output("us-map", "figure"),
    Input("category-toggle", "value"),
    Input("us-map", "clickData")
)
def update_table(category, clickData):
    # Choose dataset
    df = df_in if category == "In Review" else df_rm

    # Filter by clicked state (if any)
    if clickData and "points" in clickData:
        state = clickData["points"][0]["location"]
        df = df[df["PhysicalState"] == state]

    # Build choropleth map
    state_counts = df.groupby("PhysicalState").size().reset_index(name="Count")
    fig = px.choropleth(
        state_counts,
        locations="PhysicalState",
        locationmode="USA-states",
        color="Count",
        color_continuous_scale="Viridis",
        scope="usa",
        title=f"{category} Providers by State"
    )

    return df.to_dict("records"), fig


# --- Run server ---
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
