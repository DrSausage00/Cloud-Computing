"""
MES Monitoring UI.

Zeigt die aggregierten Maschinenmetriken aus der Silver-Schicht:
  - Kacheln je Maschine (letztes Zeitfenster)
  - Temperaturverlauf einer Maschine

Die UI haelt keinen Zustand. Jeder Request kann von einem beliebigen
Pod beantwortet werden, dadurch horizontal skalierbar.
"""

import os
from datetime import datetime

import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

import data_source

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
HISTORY_MINUTES = int(os.getenv("HISTORY_MINUTES", "15"))

app = Dash(__name__, title="MES Monitoring")

# Gunicorn startet dieses Objekt (Dockerfile)
server = app.server


@server.route("/health")
def health():
    """Endpunkt fuer Readiness- und Liveness-Probe in Kubernetes."""
    return {"status": "ok"}


def machine_tile(row: dict) -> html.Div:
    """Eine Kachel je Maschine: Mittelwert des letzten Zeitfensters."""
    warning = "Grenzwert überschritten" if row["limit_exceeded"] else ""

    return html.Div(
        className="tile tile--alarm" if row["limit_exceeded"] else "tile",
        children=[
            html.Div(row["machine_id"], className="tile__id"),
            html.Div(f"{row['avg_temperature']:.1f} °C", className="tile__value"),
            html.Div(row["last_status"], className="tile__status"),
            html.Div(warning, className="tile__warning"),
        ],
    )


def history_figure(rows: list[dict], machine_id: str) -> go.Figure:
    """Temperaturverlauf mit Grenzwertlinie."""
    figure = go.Figure()

    if not rows:
        figure.add_annotation(text="Keine Daten", showarrow=False)
    else:
        figure.add_trace(
            go.Scatter(
                x=[datetime.fromisoformat(row["window_start"]) for row in rows],
                y=[row["avg_temperature"] for row in rows],
                mode="lines",
                line={"color": "#256391"},
            )
        )
        figure.add_hline(
            y=rows[-1]["temperature_limit"],
            line={"color": "#B91C1C", "dash": "dash"},
            annotation_text="Grenzwert",
        )

    figure.update_layout(
        title=f"Temperaturverlauf {machine_id}",
        height=340,
        margin={"l": 48, "r": 24, "t": 48, "b": 40},
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        showlegend=False,
    )
    figure.update_yaxes(title="°C", gridcolor="#EAEAEA")
    return figure


app.layout = html.Div(
    className="page",
    children=[
        html.H1("MES Monitoring"),
        html.Div(id="tiles", className="tiles"),
        dcc.Dropdown(
            id="machine-picker",
            options=[m["machine_id"] for m in data_source.MACHINES],
            value=data_source.MACHINES[0]["machine_id"],
            clearable=False,
            className="picker",
        ),
        dcc.Graph(id="history", config={"displayModeBar": False}),
        dcc.Interval(id="tick", interval=POLL_INTERVAL_SECONDS * 1000),
    ],
)


@app.callback(Output("tiles", "children"), Input("tick", "n_intervals"))
def update_tiles(_):
    rows = sorted(data_source.fetch_latest(), key=lambda r: r["machine_id"])
    return [machine_tile(row) for row in rows]


@app.callback(
    Output("history", "figure"),
    Input("machine-picker", "value"),
    Input("tick", "n_intervals"),
)
def update_history(machine_id, _):
    rows = data_source.fetch_history(machine_id, minutes=HISTORY_MINUTES)
    return history_figure(rows, machine_id)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")))
