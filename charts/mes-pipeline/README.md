# Helm-Chart `mes-pipeline`

Ein Chart für die gesamte Pipeline, ein Deploy-Befehl, zwei Umgebungen (minikube, DHBW Cloud).
Einordnung in die Gesamtarchitektur: siehe Haupt-README, §8 und §9.

## Aufbau

```
charts/mes-pipeline/
├── values.yaml               # Basis-Werte, für beide Umgebungen identisch
├── values-dhbw.yaml          # überschreibt nur die Umgebungsunterschiede
└── templates/
    ├── _helpers.tpl           # gemeinsame Label-/Namens-Helfer
    ├── configmap.yaml          # pipeline-config: alle nicht-geheimen Laufzeit-Werte
    ├── secret.yaml             # minio-credentials
    ├── kafka.yaml              # StatefulSet, 3 Broker, KRaft-Modus
    ├── minio.yaml              # StatefulSet, 4 Nodes, Distributed Mode
    ├── ingestion.yaml          # 3 Deployments per Helm-range (a/b/c)
    ├── stream-processing.yaml  # 3 Deployments per Helm-range (a/b/c)
    ├── compaction-cronjob.yaml       # CronJob: Silver-Kompaktierung, alle 30 Min
    ├── bronze-compaction-cronjob.yaml # CronJob: Bronze-Kompaktierung, alle 10 Min
    ├── serving-api.yaml         # Deployment + HPA (kein von Helm verwaltetes replicas)
    ├── ui.yaml                  # Deployment + Service
    └── ci-rbac.yaml             # ServiceAccount/Role/RoleBinding für GitHub Actions
```

## Zwei Umgebungen, ein Chart

```bash
# minikube
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml --namespace mes --create-namespace --wait

# DHBW Cloud
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml -f ./charts/mes-pipeline/values-dhbw.yaml --namespace mes --create-namespace --wait
```

`values-dhbw.yaml` überschreibt ausschließlich Image-Registry, `imagePullPolicy`, UI-Service-Typ
und Ingress. Kafka mit 3 Brokern, MinIO mit 4 Knoten, die HPA auf der Serving-API und die beiden
Kompaktierungs-CronJobs sind in beiden Umgebungen exakt dasselbe.

`values-secret.yaml` (nicht im Repository, siehe `values-secret.yaml.example`) enthält nur die
MinIO-Zugangsdaten für das `Secret`.

## Horizontale Skalierung über Helm-`range`

Statt `replicas: N` erzeugen `ingestion.yaml` und `stream-processing.yaml` je ein eigenes
`Deployment` pro Eintrag in `values.yaml`:

```yaml
ingestion:
  instances:
    - name: a
      machineTypes: "A"
    - name: b
      machineTypes: "B"
    - name: c
      machineTypes: "C"
```

Der Chart iteriert mit `{{- range .Values.ingestion.instances }}` über diese Liste und erzeugt
`ingestion-a`, `ingestion-b`, `ingestion-c` als eigenständige Deployments, jedes mit eigenem
`MACHINE_TYPES`-Env-Wert. Begründung, warum nicht einfach `replicas: 3`: siehe Haupt-README §8.

Jede Instanz trägt zusätzlich zum individuellen `app`-Label ein gemeinsames
`mes.family: ingestion` (bzw. `stream-processing`) Label, damit sich alle drei Instanzen einer
Komponente weiterhin gemeinsam abfragen lassen (`kubectl get pods -l mes.family=ingestion`),
obwohl `-l app=ingestion` seit dem Split nichts mehr matcht.

## Kompaktierung als eigener Workload-Typ

`compaction-cronjob.yaml` und `bronze-compaction-cronjob.yaml` sind bewusst `CronJob`s, kein
Dauerbetrieb: Die Kompaktierung läuft nur wenige Sekunden bis Minuten alle 10–30 Minuten, ein
dauerhaft laufender Pod mit eigener Sleep-Schleife würde in der Zwischenzeit unnötig
Ressourcen binden und hätte keine eingebaute Lauf-Historie. `kubectl get cronjob` zeigt
Zeitplan und letzten Lauf auf einen Blick; `successfulJobsHistoryLimit`/`failedJobsHistoryLimit`
begrenzen, wie viele abgeschlossene Job-Pods sich ansammeln. Fachliche Details zum
Kompaktierungs-Problem und -Ablauf: Haupt-README §6.

## Konfiguration und Secrets

Alle nicht-geheimen Laufzeit-Werte (Kafka-Bootstrap, Temperaturgrenzwert, MinIO-Endpunkt,
S3-Timeouts, Cache-TTLs, Tabellenpfade) liegen zentral in einer `ConfigMap` (`configmap.yaml`),
befüllt aus `values.yaml`/`pipeline.*`. Die MinIO-Zugangsdaten liegen in einem eigenen `Secret`
(`secret.yaml`), gespeist aus `values-secret.yaml`. Kein Pod bekommt Zugangsdaten fest im
Image oder im Deployment-Manifest.

## Autoskalierung der Serving-API

`serving-api.yaml` definiert bewusst **kein** `replicas`-Feld im `Deployment` — das überlässt
Helm das Feld vollständig dem `HorizontalPodAutoscaler`. Würde Helm bei jedem `upgrade` einen
festen `replicas`-Wert zurückschreiben, würde das mit der HPA um die Pod-Zahl konkurrieren.

```yaml
servingApi:
  autoscaling:
    minReplicas: 1
    maxReplicas: 5
    targetCPUUtilization: 50
```

## CI/CD-Anbindung

`ci-rbac.yaml` legt eine eigene ServiceAccount `github-actions-restarter` mit einer `Role` an,
die ausschließlich `get/list/patch` auf `apps/deployments` im Namespace `mes` erlaubt — kein
Cluster-Admin-Zugriff für die CI-Pipeline. Details zum Zusammenspiel mit den
GitHub-Actions-Workflows: Haupt-README §9.
