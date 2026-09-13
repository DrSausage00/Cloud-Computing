# Contributing

Kurzleitfaden für alle im Team, bevor ihr pusht oder deployt.

## Projektstruktur

| Ordner | Komponente |
|---|---|
| `ingestion/` | Simuliert Maschinen, sendet Events an Kafka |
| `stream-processing/` | Spark Structured Streaming, Kafka → Bronze/Silver in MinIO |
| `serving-api/` | FastAPI, liest Silver/Status aus MinIO, stellt Endpoints bereit |
| `ui/` | Dash-Dashboard, zeigt Daten aus `serving-api` |
| `charts/mes-pipeline/` | Helm-Chart für den gesamten Stack |
| `ansible/`, `terraform/` | Infrastruktur für den DHBW-Cloud-Cluster |
| `tools/` | Betriebs-Scripts (Tunnel, Deploy) |

## Bevor du pusht

- **Keine Secrets committen.** `values-secret.yaml`, `ansible/secrets.yml`, `ansible/registry-credentials.txt` und `ansible/github-gist-token.txt` sind gitignored, das bleibt so. Für neue Secrets: `.example`-Datei mit Platzhaltern anlegen, echte Datei niemals committen.
- **`git status` vor jedem `git add -A`/`git add .` prüfen.** Windows-Checkouts zeigen manchmal Dutzende Dateien als "modified", die in Wirklichkeit nur andere Zeilenenden (CRLF/LF) haben, keine echten Änderungen. Lieber gezielt einzelne Dateien stagen (`git add <pfad>`) als pauschal alles.
- **Schema-Änderungen an Ingestion-Events** (neue/umbenannte Felder in `measurements`) wirken sich auf `stream-processing/streaming_job.py` aus (`machine_schema`, `measurement_schema`). Kurz im Team Bescheid geben, bevor sich das Format ändert.

## CI/CD

Bei einem Push auf `main`, der Dateien in `ingestion/`, `serving-api/` oder `ui/` ändert, läuft automatisch:
1. Docker-Image der jeweiligen Komponente bauen
2. In die eigene, per Basic-Auth abgesicherte Registry pushen
3. `kubectl rollout restart` für das entsprechende Deployment im Cluster

Ausnahme `stream-processing/`: Das Image ist zu groß für den Registry-Tunnel (siehe README §9), Build und Push laufen manuell; der Workflow `build-stream-processing.yml` ist nur per `workflow_dispatch` auslösbar und übernimmt den Rollout-Restart. Chart- und Config-Änderungen (`charts/`, `values*.yaml`) werden über einen Cronjob auf dem Master automatisch per `helm upgrade` ausgerollt (Prüfung alle 5 Minuten).

Manuell bauen nur, wer explizit testen will, bevor gepusht wird:
```bash
docker build -t <registry>/mes/<komponente>:0.1 <komponente>/
docker push <registry>/mes/<komponente>:0.1
```

## Deployment lokal testen (minikube)

```bash
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml --namespace mes --create-namespace --wait --timeout 10m
```

## Deployment DHBW Cloud

Läuft automatisch über den Cronjob (siehe oben). Manuell, falls nötig:
```bash
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml -f ./charts/mes-pipeline/values-dhbw.yaml --namespace mes --create-namespace --wait --timeout 10m
```

## Fehler melden statt selbst fixen

Wenn du einen Bug in der Komponente eines anderen Teammitglieds findest: melde ihn demjenigen (kurze Beschreibung plus Datei/Zeile), statt ihn selbst in dessen Code zu ändern, außer ihr sprecht das ab. Infrastruktur- und Deployment-Dateien (`charts/`, `ansible/`, `terraform/`, `tools/`) sind Integrator-Scope und können direkt angefasst werden.

## Fragen

Gruppenchat, nicht direkt am Server rumprobieren.
