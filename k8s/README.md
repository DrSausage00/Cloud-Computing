# Kubernetes-Manifeste — MES-Pipeline

Namespace: `mes`. Reihenfolge über Nummernpräfixe (Kubernetes ist deklarativ, `kubectl apply -R`
liest aber alphabetisch — die Nummern erzwingen: erst Namespace, dann Konfiguration, dann
zustandsbehaftete Dienste, dann alles, was auf sie zugreift).

| Ordner/Datei | Inhalt | Entsteht in |
|---|---|---|
| `00-namespace.yaml` | Namespace `mes` | Guide 04 |
| `config/10-configmap.yaml` | `pipeline-config` | Guide 05 |
| `config/11-secret.example.yaml` | Vorlage für `minio-credentials`, echte Datei ignoriert | Guide 05 |
| `kafka/` | StatefulSet + Headless Service | Guide 07 |
| `minio/` | StatefulSet + Service | Guide 07 |
| `ingestion/` | Deployment (kein Service — nimmt keine Verbindungen an) | Guide 04/06 |
| `stream-processing/` | Deployment, 1 Replica erzwungen (siehe README §12) | Guide 04/08 |
| `serving-api/` | Deployment + Service + HPA | Guide 04/09/11 |
| `ui/` | Deployment + NodePort-Service | Guide 04/10 |

Anwenden: `kubectl apply -R -f k8s/`

Die Komponenten selbst (`ingestion/`, `stream-processing/`, `serving-api/`, `ui/`) liegen im
Wurzelverzeichnis des Repos, nicht hier — dieser Ordner enthält ausschließlich die Betriebssicht
(wie läuft es), nicht den Anwendungscode (was tut es). Begründung: siehe
[`../docs/architecture/kappa-architektur.md`](../docs/architecture/kappa-architektur.md) und
Guide 03 im `learning`-Repo.