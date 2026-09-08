"""
MES Monitoring UI.

Zwei Ansichten auf die aggregierten Maschinenmetriken aus der
Silver-Schicht:
  - Uebersicht: Kacheln je Maschine und Temperaturverlauf
  - Messwerte:  Tabelle aller Zeitfenster mit allen Feldern

Die UI haelt keinen Zustand. Jeder Request kann von einem beliebigen
Pod beantwortet werden, deshalb ist sie horizontal skalierbar.
"""

import os
from datetime import datetime

import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html

import data_source

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
HISTORY_MINUTES = int(os.getenv("HISTORY_MINUTES", "15"))

app = Dash(__name__, title="MES Monitoring")

# Gunicorn startet dieses Objekt (siehe Dockerfile)
server = app.server


@server.route("/health")
def health():
    """Endpunkt fuer Readiness- und Liveness-Probe in Kubernetes."""
    return {"status": "ok"}


# --------------------------------------------------------------------
# Bausteine
# --------------------------------------------------------------------

def machine_tile(row: dict) -> html.Div:
    """Eine Kachel je Maschine: Kennzahlen des letzten Zeitfensters."""
    window_start = datetime.fromisoformat(row["window_start"])

    return html.Div(
        className="tile tile--alarm" if row["limit_exceeded"] else "tile",
        children=[
            html.Div(
                className="tile__head",
                children=[
                    html.Span(row["machine_id"], className="tile__id"),
                    html.Span(f"Typ {row['machine_type']}", className="tile__type"),
                ],
            ),
            html.Div(f"{row['avg_temperature']:.1f} °C", className="tile__value"),
            html.Div(row["last_status"], className="tile__status"),
            html.Div(
                "Grenzwert überschritten" if row["limit_exceeded"] else "",
                className="tile__warning",
            ),
            html.Dl(
                className="tile__facts",
                children=[
                    html.Dt("Min"),
                    html.Dd(f"{row['min_temperature']:.1f} °C"),
                    html.Dt("Max"),
                    html.Dd(f"{row['max_temperature']:.1f} °C"),
                    html.Dt("Grenzwert"),
                    html.Dd(f"{row['temperature_limit']:.0f} °C"),
                    html.Dt("Events"),
                    html.Dd(str(row["event_count"])),
                    html.Dt("Fenster"),
                    html.Dd(window_start.strftime("%H:%M:%S")),
                ],
            ),
        ],
    )


def history_figure(rows: list[dict], machine_id: str) -> go.Figure:
    """Temperaturverlauf mit Min-Max-Band und Grenzwertlinie."""
    figure = go.Figure()

    if not rows:
        figure.add_annotation(text="Keine Daten", showarrow=False)
    else:
        times = [datetime.fromisoformat(row["window_start"]) for row in rows]

        # Band zwischen Minimum und Maximum des jeweiligen Fensters
        figure.add_trace(
            go.Scatter(
                x=times + times[::-1],
                y=[r["max_temperature"] for r in rows]
                + [r["min_temperature"] for r in rows][::-1],
                fill="toself",
                fillcolor="rgba(37, 99, 145, 0.12)",
                line={"width": 0},
                hoverinfo="skip",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=times,
                y=[row["avg_temperature"] for row in rows],
                mode="lines",
                line={"color": "#256391", "width": 2},
                name="Mittelwert",
            )
        )
        figure.add_hline(
            y=rows[-1]["temperature_limit"],
            line={"color": "#B91C1C", "dash": "dash"},
            annotation_text="Grenzwert",
        )

    figure.update_layout(
        title=f"Temperaturverlauf {machine_id}",
        height=360,
        margin={"l": 48, "r": 24, "t": 48, "b": 40},
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        showlegend=False,
        hovermode="x unified",
    )
    figure.update_yaxes(title="°C", gridcolor="#EAEAEA")
    return figure


# Spalten der Messwerte-Tabelle: alle Felder, die die API liefert
TABLE_COLUMNS = [
    {"name": "Fensterbeginn", "id": "window_start"},
    {"name": "Fensterende", "id": "window_end"},
    {"name": "Maschine", "id": "machine_id"},
    {"name": "Typ", "id": "machine_type"},
    {"name": "Ø °C", "id": "avg_temperature"},
    {"name": "Min °C", "id": "min_temperature"},
    {"name": "Max °C", "id": "max_temperature"},
    {"name": "Events", "id": "event_count"},
    {"name": "Status", "id": "last_status"},
    {"name": "Grenzwert", "id": "temperature_limit"},
    {"name": "Überschritten", "id": "limit_exceeded"},
]


def table_rows(rows: list[dict]) -> list[dict]:
    """Zeitstempel kuerzen und Wahrheitswerte lesbar machen."""
    prepared = []
    for row in rows:
        entry = dict(row)
        for key in ("window_start", "window_end"):
            entry[key] = datetime.fromisoformat(row[key]).strftime("%H:%M:%S")
        entry["limit_exceeded"] = "ja" if row["limit_exceeded"] else "nein"
        prepared.append(entry)
    return prepared


# --------------------------------------------------------------------
# Layout
# --------------------------------------------------------------------

app.layout = html.Div(
    className="page",
    children=[
        html.H1("MES Monitoring"),
        dcc.Tabs(
            value="overview",
            className="tabs",
            children=[
                dcc.Tab(
                    label="Übersicht",
                    value="overview",
                    children=[
                        html.Div(id="tiles", className="tiles"),
                        dcc.Dropdown(
                            id="machine-picker",
                            clearable=False,
                            className="picker",
                        ),
                        dcc.Graph(id="history", config={"displayModeBar": False}),
                    ],
                ),
                dcc.Tab(
                    label="Messwerte",
                    value="table",
                    children=[
                        html.P(
                            f"Aggregierte Zeitfenster der letzten {HISTORY_MINUTES} "
                            "Minuten, neueste zuerst.",
                            className="hint",
                        ),
                        dash_table.DataTable(
                            id="measurements",
                            columns=TABLE_COLUMNS,
                            sort_action="native",
                            filter_action="native",
                            page_size=15,
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "fontFamily": "system-ui, sans-serif",
                                "fontSize": "13px",
                                "padding": "6px 10px",
                                "textAlign": "left",
                            },
                            style_header={
                                "backgroundColor": "#F4F5F6",
                                "fontWeight": "600",
                            },
                            style_data_conditional=[
                                {
                                    "if": {
                                        "filter_query": "{limit_exceeded} = ja",
                                    },
                                    "backgroundColor": "#FDECEC",
                                }
                            ],
                        ),
                    ],
                ),
            ],
        ),
        # Polling: loest die Callbacks unten regelmaessig neu aus
        dcc.Interval(id="tick", interval=POLL_INTERVAL_SECONDS * 1000),
    ],
)


# --------------------------------------------------------------------
# Callbacks
# --------------------------------------------------------------------

@app.callback(Output("tiles", "children"), Input("tick", "n_intervals"))
def update_tiles(_):
    rows = sorted(data_source.fetch_latest(), key=lambda r: r["machine_id"])
    return [machine_tile(row) for row in rows]


@app.callback(
    Output("machine-picker", "options"),
    Output("machine-picker", "value"),
    Input("tick", "n_intervals"),
    Input("machine-picker", "value"),
)
def update_machine_options(_, current):
    """Maschinenliste kommt aus den Daten, nicht aus einer festen Liste."""
    machine_ids = data_source.fetch_machine_ids()
    if not machine_ids:
        return [], None
    value = current if current in machine_ids else machine_ids[0]
    return machine_ids, value


@app.callback(
    Output("history", "figure"),
    Input("machine-picker", "value"),
    Input("tick", "n_intervals"),
)
def update_history(machine_id, _):
    if not machine_id:
        return history_figure([], "—")
    rows = data_source.fetch_history(machine_id, minutes=HISTORY_MINUTES)
    return history_figure(rows, machine_id)


@app.callback(Output("measurements", "data"), Input("tick", "n_intervals"))
def update_table(_):
    return table_rows(data_source.fetch_history_all(minutes=HISTORY_MINUTES))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")))
