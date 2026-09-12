"""
MES Monitoring UI.

Seiten:
  /                  Uebersicht: alle Maschinen als Ampel-Kacheln
  /machine/<id>      Detail: Verlauf und Kennzahlen einer Maschine
  /messwerte         Tabelle aller Zeitfenster

"""

import os
from datetime import datetime

import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html, no_update

import data_source

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
HISTORY_MINUTES = int(os.getenv("HISTORY_MINUTES", "15"))

app = Dash(__name__, title="MES Monitoring", suppress_callback_exceptions=True, update_title=None)

# Gunicorn startet dieses Objekt (siehe Dockerfile)
server = app.server


@server.route("/health")
def health():
    """Endpunkt fuer Readiness- und Liveness-Probe in Kubernetes."""
    return {"status": "ok"}


# --------------------------------------------------------------------
# Ampel
# --------------------------------------------------------------------

def traffic_light(row: dict) -> tuple[str, str]:
    """Ampelstufe und Klartext zu einer Maschine.

    rot   = Maschine steht oder meldet einen Fehler
    gelb  = laeuft, aber ueber dem Temperaturgrenzwert
    gruen = laeuft im Sollbereich
    """
    if row["last_status"] in ("ERROR", "STOPPED"):
        return "alarm", f"Status {row['last_status']}"
    if row["limit_exceeded"]:
        return "warn", "Grenzwert überschritten"
    return "ok", "Läuft im Sollbereich"


# --------------------------------------------------------------------
# Bausteine
# --------------------------------------------------------------------

def machine_tile(row: dict) -> dcc.Link:
    """Kachel je Maschine, verlinkt auf die Detailseite."""
    state, state_text = traffic_light(row)

    return dcc.Link(
        href=f"/machine/{row['machine_id']}",
        className=f"tile tile--{state}",
        children=[
            html.Div(
                className="tile__head",
                children=[
                    html.Span(className=f"dot dot--{state}"),
                    html.Span(row["machine_id"], className="tile__id"),
                    html.Span(f"Typ {row['machine_type']}", className="tile__type"),
                ],
            ),
            html.Div(f"{row['avg_temperature']:.1f} °C", className="tile__value"),
            html.Div(state_text, className=f"tile__state tile__state--{state}"),
            html.Dl(
                className="tile__facts",
                children=[
                    html.Dt("Min"),
                    html.Dd(f"{row['min_temperature']:.1f} °C"),
                    html.Dt("Max"),
                    html.Dd(f"{row['max_temperature']:.1f} °C"),
                    html.Dt("Events"),
                    html.Dd(str(row["event_count"])),
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
        title=f"Temperaturverlauf der letzten {HISTORY_MINUTES} Minuten",
        height=380,
        margin={"l": 48, "r": 24, "t": 48, "b": 40},
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        showlegend=False,
        hovermode="x unified",
    )
    figure.update_yaxes(title="°C", gridcolor="#EAEAEA")
    return figure


def detail_facts(row: dict | None) -> html.Div:
    """Kennzahlenzeile oben auf der Detailseite."""
    if row is None:
        return html.Div("Keine aktuellen Daten für diese Maschine.", className="hint")

    state, state_text = traffic_light(row)
    window_start = datetime.fromisoformat(row["window_start"])

    entries = [
        ("Mittelwert", f"{row['avg_temperature']:.1f} °C"),
        ("Minimum", f"{row['min_temperature']:.1f} °C"),
        ("Maximum", f"{row['max_temperature']:.1f} °C"),
        ("Grenzwert", f"{row['temperature_limit']:.0f} °C"),
        ("Events im Fenster", str(row["event_count"])),
        ("Status", row["last_status"]),
        ("Letztes Fenster", window_start.strftime("%H:%M:%S")),
        ("Typ", row["machine_type"]),
    ]

    return html.Div(
        children=[
            html.Div(
                className=f"banner banner--{state}",
                children=[
                    html.Span(className=f"dot dot--{state}"),
                    html.Span(state_text),
                ],
            ),
            html.Dl(
                className="facts",
                children=[
                    element
                    for label, value in entries
                    for element in (html.Dt(label), html.Dd(value))
                ],
            ),
        ]
    )


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
# Seiten
# --------------------------------------------------------------------

def page_overview() -> html.Div:
    return html.Div(
        children=[
            html.P(
                "Alle Maschinen im Überblick. Kachel anklicken für Details.",
                className="hint",
            ),
            html.Div(id="tiles", className="tiles"),
        ]
    )


def page_detail(machine_id: str) -> html.Div:
    return html.Div(
        children=[
            dcc.Link("← Zurück zur Übersicht", href="/", className="back"),
            html.H2(machine_id, className="detail__title"),
            html.Div(id="detail-facts"),
            dcc.Graph(id="history", config={"displayModeBar": False}),
        ]
    )


def page_table() -> html.Div:
    return html.Div(
        children=[
            html.P(
                f"Aggregierte Zeitfenster der letzten {HISTORY_MINUTES} Minuten, "
                "neueste zuerst. Spalten sind sortier- und filterbar.",
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
                style_header={"backgroundColor": "#F4F5F6", "fontWeight": "600"},
                style_data_conditional=[
                    {
                        "if": {"filter_query": "{limit_exceeded} = ja"},
                        "backgroundColor": "#FDECEC",
                    }
                ],
            ),
        ]
    )


app.layout = html.Div(
    className="page",
    children=[
        dcc.Location(id="url"),
        html.Header(
            className="page__head",
            children=[
                html.H1("MES Monitoring"),
                html.Nav(
                    className="nav",
                    children=[
                        dcc.Link("Übersicht", href="/", className="nav__link"),
                        dcc.Link("Messwerte", href="/messwerte", className="nav__link"),
                    ],
                ),
            ],
        ),
        html.Div(id="content"),
        # Polling: loest die Callbacks unten regelmaessig neu aus
        dcc.Interval(id="tick", interval=POLL_INTERVAL_SECONDS * 1000),
    ],
)


# --------------------------------------------------------------------
# Callbacks
# --------------------------------------------------------------------

@app.callback(Output("content", "children"), Input("url", "pathname"))
def route(pathname):
    """Welche Seite wird angezeigt? Der Zustand steckt in der URL."""
    if pathname and pathname.startswith("/machine/"):
        return page_detail(pathname.removeprefix("/machine/"))
    if pathname == "/messwerte":
        return page_table()
    return page_overview()


@app.callback(
    Output("tiles", "children"),
    Input("tick", "n_intervals"),
    Input("url", "pathname"),
)
def update_tiles(_, pathname):
    if pathname not in (None, "/"):
        return no_update
    rows = sorted(data_source.fetch_latest(), key=lambda r: r["machine_id"])
    if not rows:
        return html.P("Keine Maschinendaten verfügbar.", className="hint")
    return [machine_tile(row) for row in rows]


@app.callback(
    Output("detail-facts", "children"),
    Output("history", "figure"),
    Input("tick", "n_intervals"),
    Input("url", "pathname"),
)
def update_detail(_, pathname):
    if not pathname or not pathname.startswith("/machine/"):
        return no_update, no_update

    machine_id = pathname.removeprefix("/machine/")
    latest = next(
        (r for r in data_source.fetch_latest() if r["machine_id"] == machine_id),
        None,
    )
    history = data_source.fetch_history(machine_id, minutes=HISTORY_MINUTES)
    return detail_facts(latest), history_figure(history, machine_id)


@app.callback(
    Output("measurements", "data"),
    Input("tick", "n_intervals"),
    Input("url", "pathname"),
)
def update_table(_, pathname):
    if pathname != "/messwerte":
        return no_update
    return table_rows(data_source.fetch_history_all(minutes=HISTORY_MINUTES))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")))
