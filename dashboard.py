import pandas as pd
import dash
from dash import dash_table, dcc, html, Input, Output, State
import plotly.express as px
from pathlib import Path
import io
import base64

# --- Load master CSVs ---
outputs_dir = Path("outputs")
in_review_file = outputs_dir / "master_fmcsa_in_review.csv"
removed_file = outputs_dir / "master_fmcsa_removed.csv"

df_in = pd.read_csv(in_review_file, dtype=str)
df_rm = pd.read_csv(removed_file, dtype=str)

# Ensure key columns exist
for df in [df_in, df_rm]:
    if "UpdatedOn" not in df.columns:
        df["UpdatedOn"] = ""
    if "RemovedReason" not in df.columns:
        df["RemovedReason"] = ""

# --- Track transfer month ---
df_transfer = df_in.merge(
    df_rm,
    on="ProviderNumber",
    how="inner",
    suffixes=("_in", "_rm")
)
if not df_transfer.empty:
    df_transfer["TransferMonth"] = pd.to_datetime(
        df_transfer["UpdatedOn_rm"], errors="coerce"
    ).dt.to_period("M").astype(str)
else:
    df_transfer["TransferMonth"] = []

# --- Add TransferMonth column to removed df for reference ---
df_rm = df_rm.merge(
    df_transfer[["ProviderNumber", "TransferMonth"]],
    on="ProviderNumber",
    how="left"
)

# --- Optional: sort by state and zip ---
df_in = df_in.sort_values(["PhysicalState", "ZipCode"])
df_rm = df_rm.sort_values(["PhysicalState", "ZipCode"])

# --- Initialize Dash ---
app = dash.Dash(__name__)
app.title = "FMCSA Provider Dashboard"

# --- Layout ---
app.layout = html.Div([
    html.H1("FMCSA Provider Dashboard", style={"textAlign": "center"}),

    # Toggle bar for category
    html.Div([
        html.Label("Select Category:"),
        dcc.RadioItems(
            ["In Review", "Removed"],
            "In Review",
            id="category-toggle",
            inline=True
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # Table
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

    # Map
    dcc.Graph(id="us-map"),

    html.Br(),

    # Download button
    html.Div([
        html.Button("Download Filtered CSV", id="download-btn"),
        dcc.Download(id="download-data")
    ], style={"textAlign": "center"})
])


# --- Callbacks ---

@app.callback(
    Output("provider-table", "data"),
    Output("us-map", "figure"),
    Input("category-toggle", "value"),
    Input("us-map", "clickData")
)
def update_table(category, clickData):
    # Select dataset
    df = df_in.copy() if category == "In Review" else df_rm.copy()

    # Filter by clicked state
    if clickData and "points" in clickData:
        state = clickData["points"][0]["location"]
        df = df[df["PhysicalState"] == state]

    # Build choropleth
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


@app.callback(
    Output("download-data", "data"),
    Input("download-btn", "n_clicks"),
    State("provider-table", "data"),
    prevent_initial_call=True
)
def download_filtered(n_clicks, table_data):
    if table_data:
        df = pd.DataFrame(table_data)
        # Convert to CSV in-memory
        csv_string = df.to_csv(index=False, encoding="utf-8")
        return dcc.send_string(csv_string, filename="filtered_providers.csv")
    return None


# --- Run server ---
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
