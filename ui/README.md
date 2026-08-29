# UI — MES Monitoring

Weboberfläche der Pipeline. Zeigt die aggregierten Maschinenmetriken aus der
Silver-Schicht (`silver_machine_metrics`, siehe `ingestion/storage/schema.sql`).

Rolle laut Aufgabenstellung: **Anzeige der verarbeiteten Ergebnisse.**

## Aufbau

| Datei | Zweck |
|---|---|
| `app.py` | Dash-App: Layout, Live-Kacheln, Verlaufs-Chart, Callbacks |
| `data_source.py` | Einzige Stelle, an der Daten geholt werden. Mock oder echte API |
| `assets/style.css` | Styling, wird von Dash automatisch geladen |
| `Dockerfile` | Container-Image, startet Gunicorn |

## Datenanbindung

Die UI liest ausschließlich über die Serving-API. Sie schreibt nichts und
spricht weder Kafka noch MinIO direkt an.

Solange die API noch nicht steht, liefert `data_source.py` erfundene Daten im
Format der Silver-Tabelle. Umstellung auf die echte API: `USE_MOCK=false`
setzen und `API_BASE_URL` auf den Service zeigen lassen. Der Rest der UI bleibt
unverändert.

Erwartete Endpunkte:

- `GET /metrics/latest` → Liste, ein Objekt je Maschine (neuestes Fenster)
- `GET /metrics/history?machine_id=A-001&minutes=15` → Liste von Fenstern

Feldnamen entsprechen den Spalten von `silver_machine_metrics`.

## Aktualisierung

Polling über `dcc.Interval`, Standard alle 5 Sekunden. Das passt zum
10-Sekunden-Aggregationsfenster im Processing. Bewusst kein WebSocket und keine
Server-Sent Events: dauerhaft offene Verbindungen erschweren das horizontale
Skalieren der UI-Pods, ohne bei dieser Datenrate einen Vorteil zu bringen.

## Lokal starten

```bash
pip install -r requirements.txt
python app.py          # http://localhost:8050
```

Als Container:

```bash
docker build -t mes-ui .
docker run -p 8050:8050 -e USE_MOCK=true mes-ui
```

---

## Steckbrief für das Kubernetes-Deployment

| Frage | Antwort |
|---|---|
| Interner Port | `8050` |
| Health-Endpunkt | `GET /health` → `{"status": "ok"}`, für Readiness- und Liveness-Probe |
| Secrets | keine — die UI liest nur, ohne Authentifizierung |
| PVC | nein — die UI hält keinen Zustand auf Platte |
| Replicas | 2 als Default, für den Skalierungsnachweis auf 3+ hochziehen |
| Workload-Typ | `Deployment` (kein StatefulSet, da zustandslos) |

Umgebungsvariablen, alle über ConfigMap setzbar:

| Variable | Default | Zweck |
|---|---|---|
| `API_BASE_URL` | `http://serving-api:8000` | Basis-URL der Serving-API |
| `USE_MOCK` | `true` | `false`, sobald die API steht |
| `POLL_INTERVAL_SECONDS` | `5` | Abstand zwischen den Abfragen |
| `HISTORY_MINUTES` | `15` | Zeitraum im Verlaufs-Chart |
| `PORT` | `8050` | nur für den lokalen Entwicklungsserver |

**Zur Skalierbarkeit:** Die UI hält keinen Sitzungszustand im Prozess. Jeder
Request kann von einem beliebigen Pod beantwortet werden, deshalb braucht der
Service keine Session Affinity. Im Container laufen zusätzlich zwei
Gunicorn-Worker pro Pod.
