## Inhaltsverzeichnis

- [1. Use Case und Motivation](#1-use-case-und-motivation)
- [2. Datencharakteristik](#2-datencharakteristik)
- [3. Architekturentscheidung: Kappa](#3-architekturentscheidung-kappa)
- [4. Komponenten und Datenfluss](#4-komponenten-und-datenfluss)
- [5. Processing-Logik](#5-processing-logik)
- [6. Speicherkonzept](#6-speicherkonzept)
- [7. User-facing UI](#7-user-facing-ui)
- [8. Kubernetes-Deployment](#8-kubernetes-deployment)
- [9. Deployment-Anleitung](#9-deployment-anleitung)
- [10. Wesentliche Codeabschnitte](#10-wesentliche-codeabschnitte)
- [11. Screenshots und Nachweise](#11-screenshots-und-nachweise)
- [12. Grenzen und Ausblick](#12-grenzen-und-ausblick)

---

## 1. Use Case und Motivation

Eine Fertigung betreibt Maschinen unterschiedlicher Generationen und Hersteller. Jede meldet
Betriebsdaten — Temperatur, Druck, Vibration, Status —, aber jede in ihrem **eigenen Rohformat**,
so wie es in gewachsenen Werkslandschaften tatsächlich ist. Ein *Manufacturing Execution System*
(MES) führt diese Ströme zusammen und überwacht sie nahezu in Echtzeit: Läuft jede Maschine?
Überschreitet eine die Temperaturgrenze? Wie sah der Verlauf der letzten Stunde aus?

**Warum das ein Big-Data-Problem ist** — nicht wegen der Datenmenge des Prototyps, die ist klein,
sondern wegen der Struktur des Problems:

- **Unbegrenzter Datenstrom** statt abgeschlossener Datei. Es gibt kein „Ende" der Daten, also
  auch keinen Zeitpunkt, an dem man „alles" auswerten könnte. Auswertung muss *laufend* geschehen.
- **Heterogenität als Normalfall.** Drei Formate heute, zehn morgen. Normalisierung muss eine
  eigene Architekturschicht sein, kein `if`-Zweig.
- **Entkopplung.** Datenerzeuger, Verarbeitung und Anzeige dürfen sich nicht gegenseitig
  blockieren. Genau dafür sitzt ein Broker dazwischen.
- **Horizontale Skalierung.** Mehr Maschinen dürfen mehr Instanzen bedeuten, aber keine
  Neuentwicklung.

Die Architektur ist damit auf ein Volumen ausgelegt, das der Prototyp bewusst nicht erzeugt.

---

## 2. Datencharakteristik

### Volume

Der Simulator erzeugt je Durchlauf 10 Events von Maschinentyp A, 5 von B und 3 von C und
pausiert dann zwei Sekunden — also **18 Events alle 2 Sekunden**.

| Größe | Prototyp | Hochgerechnet (500 Maschinen, 1 Hz) |
|---|---|---|
| Ereignisrate | 9 Events/s | 500 Events/s |
| Events pro Tag | ≈ 778.000 | ≈ 43.200.000 |
| Rohvolumen/Tag (JSON ≈ 160 B) | ≈ 124 MB | ≈ 6,9 GB |
| Aggregate/Tag (10-s-Fenster) | ≈ 26.000 Zeilen | ≈ 4,3 Mio. Zeilen |

124 MB am Tag sind kein Big Data. Die Architektur trägt die rechte Spalte, der Prototyp erzeugt
die linke — das ist eine bewusste Entscheidung und keine Auslassung.

### Velocity

Kontinuierlicher Strom ohne Ende. Die Verarbeitung erfolgt in 10-Sekunden-Fenstern über
Event-Time mit einer Watermark von 20 Sekunden; verspätete Ereignisse innerhalb dieses Fensters
werden noch berücksichtigt, spätere verworfen.

### Variety

Drei Rohformate werden in der Ingestion auf ein einheitliches Schema normalisiert. Das ist der
eigentliche fachliche Kern des Use Case:

| Maschinentyp | Format | Beispiel |
|---|---|---|
| A | JSON | `{"timestamp": "...", "machine_id": "A-001", "temperature": 78.3, "pressure": 4.1, "rotation_speed": 1500.2, "power_consumption": 12.4}` |
| B | JSON, andere Feldnamen | `{"ts": "...", "id": "B-001", "temp": 65.8, "vibration": 3.2}` |
| C | Pipe-separiert | `2026-09-07T14:00:00\|C-001\|88.1\|RUNNING` |

Schon zwischen A und B unterscheiden sich die Feldnamen (`timestamp`/`ts`, `machine_id`/`id`,
`temperature`/`temp`) — die Normalisierung darf sich also nicht auf gleiche Schlüssel verlassen.

### Veracity

Nicht jedes Ereignis trägt jedes Feld. Nur Maschinentyp C meldet einen Status, Druck und
Vibration sind nicht bei allen Typen vorhanden. Die Verarbeitung muss mit fehlenden Feldern
umgehen, statt sie vorauszusetzen.

---

## 3. Architekturentscheidung: Kappa

Wir setzen eine **Kappa-Architektur** um: ein einziger Verarbeitungspfad für alle Daten,
Streaming als Standardfall. Kein separater Batch-Zweig.

**Warum nicht Lambda.** Lambda führt zwei Pfade parallel, einen für Echtzeit und einen für
Genauigkeit. Der Preis ist, dieselbe Logik zweimal zu implementieren und konsistent zu halten —
für ein Team dieser Größe und einen Anwendungsfall, der ohnehin auf Aktualität zielt, ein
schlechtes Verhältnis. Historische Auswertungen decken wir stattdessen über die Kafka-Retention
und das Parquet-Archiv ab.

**Die Umsetzung ist „Practical Kappa": Log plus Objektspeicher-Archiv.**

| Medallion-Schicht | Üblicherweise | Bei uns |
|---|---|---|
| **Bronze** — Rohdaten | im Data Lake | **im Kafka-Log**, Retention 7 Tage. In Kappa ist der Log die Rohschicht |
| **Silver** — bereinigt, aggregiert | im Data Lake | `mes-data/silver/machine-metrics` als Parquet |
| **Gold** — kuratiert, geschäftsnah | im Data Lake | **nicht materialisiert** — die Serving-API aggregiert beim Lesen |

Die Grenze der Bronze-Schicht ist ehrlich zu benennen: Nach sieben Tagen ist die Rohhistorie
weg. Für den Prototyp ist das vertretbar, für einen Produktivbetrieb wäre eine zusätzliche
Ablage der Rohereignisse nötig.

### Einordnung ins Lakehouse-Schichtenmodell

| Schicht | Beispiele der Vorlesung | Unsere Wahl |
|---|---|---|
| Compute Engine | Spark, Flink, Trino | **Spark Structured Streaming** |
| Katalog | Hive Metastore, Unity Catalog | **keiner** — bewusste Lücke |
| Open Table Format | Delta, Iceberg, Hudi | **keines** — bewusste Lücke |
| Dateiformat | Parquet, ORC | **Parquet** |
| Objektspeicher | S3, SeaweedFS, HDFS | **MinIO** |

Vier von fünf Schichten sind besetzt. Die beiden offenen sind der Grund, warum wir kein ACID und
keine Schema-Evolution haben — siehe §12.

---

## 4. Komponenten und Datenfluss

```
Ingestion  ──▶  Kafka  ──▶  Stream Processing  ──▶  MinIO  ──▶  Serving-API  ──▶  UI
 3 Formate     Puffer,      Fenster, Watermark,     Parquet,     Abfragen        Anzeige
 normalisiert  Retention    State                   Silver
```

| Komponente | Ordner | Verantwortlich | Aufgabe |
|---|---|---|---|
| Ingestion | [`ingestion/`](ingestion/) | Leo, Kirill | Simulatoren, Normalisierung, Kafka-Producer |
| Kafka | — | Kirill | Broker im KRaft-Modus (3 Broker), Topics, Retention |
| Stream Processing | [`stream-processing/`](stream-processing/) — [README](stream-processing/README.md) | Cäcilia | Spark Structured Streaming |
| MinIO | — | Kirill | S3-kompatibler Objektspeicher (4-Node Distributed Mode), Silver-Schicht |
| Serving-API | [`serving-api/`](serving-api/) — [README](serving-api/Readme.md) | Aaron | Abfrage-Endpunkte über der Silver-Schicht |
| UI | [`ui/`](ui/) — [README](ui/README.md) | Max | Dashboard |
| Deployment | [`charts/mes-pipeline/`](charts/mes-pipeline/) — [README](terraform/README.md) | Lars | Helm-Chart, Cluster-Bereitstellung (Terraform + k3s) |

**Warum ein Broker dazwischen.** Ohne Kafka wären Ingestion und Verarbeitung fest gekoppelt: Ein
kurzer Ausfall der Verarbeitung würde Ereignisse verlieren, und eine langsame Verarbeitung würde
die Erzeugung ausbremsen. Mit Kafka warten die Ereignisse, die Verarbeitung holt sie nach, und
ein fehlerhafter Lauf lässt sich durch Zurücksetzen des Offsets auf **denselben** Daten
wiederholen.

**Warum `machine_id` als Kafka-Key.** Derselbe Key landet immer in derselben Partition. Dadurch
bleibt die zeitliche Reihenfolge je Maschine garantiert, auch wenn mehrere Konsumenten parallel
lesen. Die Partitionszahl ist damit gleichzeitig die Obergrenze der Parallelität.

---

## 5. Processing-Logik

Der Streaming-Job liest aus Kafka, aggregiert über Event-Time-Fenster und schreibt nach MinIO.

| Baustein | Umsetzung |
|---|---|
| Fenster | 10 Sekunden über Event-Time |
| Late Data | Watermark von 20 Sekunden |
| Aggregate | Durchschnitts-, Minimal- und Maximaltemperatur, Event Count |
| State | letzter bekannter Maschinenstatus |
| Anreicherung | Grenzwert `TEMP_LIMIT` aus der ConfigMap, Flag `limit_exceeded` |
| Ausgabe | Parquet in `mes-data/silver/machine-metrics`, partitioniert nach `machine_type`/`event_date` |
| Wiederanlauf | Checkpoints in `spark-checkpoints`, je Query ein eigener Pfad |

**Warum Event-Time und nicht Verarbeitungszeit.** Ein Ereignis, das wegen einer Netzstörung
zehn Sekunden später ankommt, gehört fachlich in das Fenster seiner Entstehung, nicht in das
seiner Ankunft. Nur so bleiben die Aggregate über Wiederholungen hinweg identisch.

**Warum eine Watermark nötig ist.** Ohne sie müsste Spark jedes Fenster unbegrenzt offen halten,
falls doch noch ein spätes Ereignis kommt — der Zustand würde monoton wachsen. Die Watermark ist
die explizite Zusage: Nach 20 Sekunden wird ein Fenster geschlossen, Späteres wird verworfen.

---

## 6. Speicherkonzept

### Format

**Parquet.** Spaltenorientiert, komprimiert und mit eingebettetem Schema. Für die Abfragen der
Serving-API — „Durchschnittstemperatur je Maschine der letzten Stunde" — werden nur wenige
Spalten gelesen; ein zeilenorientiertes Format wie CSV oder JSON müsste jedes Mal alles lesen.

### Retention

Kafka hält die Rohereignisse 7 Tage (168 h). Das ist gleichzeitig unsere Bronze-Schicht
(siehe §3) und das Zeitfenster, innerhalb dessen sich eine Verarbeitung per Offset-Reset
wiederholen lässt.

---

## 7. User-facing UI

Die Weboberfläche zeigt die Maschinenübersicht der Pipeline live an — eine Kachel je Maschine
mit den aktuellen Aggregatwerten aus der Silver-Schicht, dazu ein Temperaturverlauf im Detail.

**Datenanbindung.** Die UI spricht ausschließlich mit der Serving-API (`GET /metrics/latest` für
die Übersicht, `GET /metrics/history` für den Verlauf) — kein direkter Zugriff auf Kafka, Spark
oder MinIO. Das hält das Architekturdiagramm konsistent zum tatsächlichen Datenfluss.

| Element | Feld | Warum es überzeugt |
|---|---|---|
| Kachel je Maschine | `machine_id`, `machine_type` | zeigt, dass alle drei Rohformate ankommen |
| Aktuelle Temperatur | `avg_temperature`, `min_temperature`, `max_temperature` | zeigt die Aggregation |
| **Warnung bei Überhitzung** | `limit_exceeded` | zeigt die ConfigMap-Anreicherung |
| Status | `last_status` | zeigt den Streaming-State |
| Messwerte im Fenster | `event_count` | zeigt Windowing und Datenausfall |
| Zeitreihe | `/metrics/history` | zeigt das Data Lake |

Die Warnung bei `limit_exceeded` ist das stärkste Element: Sie beweist in einem einzigen
Screenshot, dass ein Wert aus einer Kubernetes-ConfigMap durch einen Spark-Job bis in die
Oberfläche wirkt.

**Bedienablauf.** Übersichtsseite mit allen Maschinen als Kacheln (Ampel-Farbe nach Status/
`limit_exceeded`) — Klick auf eine Kachel führt zur Detailseite dieser einen Maschine mit
Temperaturverlauf. Echte Navigation über die URL (`/machine/<id>`), kein Dropdown-Zustand auf
einer einzelnen Seite.

---

## 8. Kubernetes-Deployment

**Aus der Aufgabenstellung:** „Skalierbarkeit: die Anwendung muss darauf ausgelegt sein, in
allen Komponenten horizontal zu skalieren und dies soll gezeigt werden."

| Komponente | Zustand | Mechanismus | Skalierungseinheit | Nachweis |
|---|---|---|---|---|
| Ingestion | zustandslos | Deployment-Replicas | Simulator-Instanz | `kubectl scale`, Offsets gemessen |
| Serving-API | zustandslos | **HPA** auf CPU | HTTP-Request | `kubectl get hpa -w` |
| UI | zustandslos | Deployment-Replicas | HTTP-Request | `kubectl scale` |
| Stream Processing | Checkpoint in MinIO | **Spark-Executor-Pods** | **Kafka-Partition** | Executor-Pods erscheinen |
| Kafka | StatefulSet + PVC | **3 KRaft-Broker** | Partition | `kafka-topics --describe`: Partitionen mit unterschiedlichen Leadern (0, 1, 2) |
| MinIO | StatefulSet + PVC | **4-Node Distributed Mode**, Erasure Coding | Knoten | `mc admin info`: vier Knoten online, EC:2 |

**Eine bewusste Grenze:** Mehr Spark-Executors als Kafka-Partitionen bringen nichts. Drei
Partitionen heißen: sinnvolle Parallelität endet bei drei Executors. Wer weiter skalieren will,
erhöht zuerst die Partitionszahl — nicht die Executor-Zahl.

Bei genügend Last werden MinIO oder Kafka zum Engpass, nicht die Serving-API.

**Eine zweite, unabhängige Grenze — lokale Testumgebung, nicht die Cloud:** Kafka (3 Broker) und
MinIO (4 Knoten) gleichzeitig auf einer einzigen minikube-VM sättigen deren Disk-I/O
(beobachtet: ~32 % I/O-Wait, Load-Average 45-70 auf 16 Kernen) — der Node wechselt kurzzeitig
zwischen `Ready`/`NotReady`, einzelne Pods (auch `metrics-server`) werden neu gestartet, weil
ihre Probes wegen des I/O-Rückstaus timeouten. Kein Konfigurationsfehler und keine
Instabilität der Anwendung selbst — sieben Storage-Pods, die sich eine gemeinsame virtuelle
Platte teilen, sind ein reines Kapazitätsproblem der lokalen Entwicklungsumgebung. Auf der DHBW
Cloud verteilt sich dieselbe I/O-Last über drei echte VMs mit jeweils eigener Platte, wo dieses
Muster in der Form nicht zu erwarten ist.

---

## 9. Deployment-Anleitung

### Voraussetzungen

| Werkzeug | Zweck |
|---|---|
| Docker | Images bauen |
| kubectl, Helm | Deployment |
| minikube | lokale Entwicklungsumgebung |
| Terraform, Ansible, OpenStack-CLI | Cluster in der DHBW Cloud |

### Ein Chart, zwei Umgebungen

```bash
# minikube
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml --namespace mes --create-namespace --wait

# DHBW Cloud
helm upgrade --install mes ./charts/mes-pipeline -f values-secret.yaml -f ./charts/mes-pipeline/values-dhbw.yaml --namespace mes --create-namespace --wait
```

`values-dhbw.yaml` überschreibt nur die paar Werte, die sich zwischen den Umgebungen
unterscheiden — Image-Registry, `imagePullPolicy`, UI-Service-Typ und Ingress. Alles andere
(Kafka mit 3 Brokern, MinIO mit 4 Knoten, die HPA auf der Serving-API) ist exakt dasselbe Chart.

### DHBW Cloud

**1. Zugang.** Die Cloud ist nur aus dem DHBW-Netz erreichbar (Eduroam oder VPN) und rein IPv6.
Die Anmeldung von Werkzeugen erfolgt über ein **Application Credential**, nicht über Benutzername
und Passwort — die Anmeldung an der Weboberfläche läuft über SSO, ein Passwort existiert dafür
nicht. Die erzeugte `clouds.yaml` liegt außerhalb des Repositories.

**2. Infrastruktur.** Details dazu in [`terraform/README.md`](terraform/README.md).

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Erzeugt drei VMs im Netz `DHBWV6` — einen Master (`general.medium`, 4 vCPU) und zwei Worker
(`general.small`, 2 vCPU, macht 8 vCPU insgesamt) — und schreibt aus deren Adressen unmittelbar
das Ansible-Inventar sowie `cluster.env`. Das ist der Kern des Infrastructure-as-Code-Gedankens:
**Die Ausgabe des einen Werkzeugs ist die Eingabe des nächsten**, es wird keine Adresse von Hand
übertragen.

Sechs Anpassungen gegenüber der Vorlage aus dem Übungs-Track waren nötig:

| | Vorlage | Hier | Warum |
|---|---|---|---|
| Anmeldung | Benutzer + Passwort | Application Credential | SSO, es gibt kein Passwort mehr |
| Netz | `DHBW-1-Upper` | `DHBWV6` | in der neuen Cloud umbenannt |
| Image | feste `image_id` | Auflösung über den Namen | die IDs der Vorlage existieren nicht mehr; die DHBW ersetzt Images regelmäßig |
| Flavor | `m1.extra_large` (8 vCPU) für alle Knoten | `general.medium`/`general.small` | 3 × 8 vCPU je Gruppe erschöpften das gemeinsame Projektkontingent (100 vCPU für den ganzen Kurs) |
| Adressen | `fixed_ip_v4` | `fixed_ip_v6`, `ip_family: ipv6` | die privaten IPv4 sind von außen nicht erreichbar |
| Kontingent nachträglich anpassen | — | `terraform apply -replace=<resource>` | OpenStack erlaubt Resize nur nach oben; Disk-Verkleinerung braucht einen Neubau |

**3. Kubernetes.** k3s wird nicht manuell installiert, sondern über die offizielle, öffentlich
verfügbare Ansible-Rolle
[`k3s-dhbw-cloud-role`](https://github.com/pfisterer/k3s-dhbw-cloud-role) — verlinkt aus der
Vorlesung „Cloud Infrastructures and Cloud Native Applications":

```bash
ansible-galaxy install -r requirements.yml
ansible-galaxy collection install kubernetes.core
ansible-playbook ansible/playbook.yml \
  -i terraform/generated-inventory.yml \
  -i ansible/overrides.yml
```

Die Rolle erkennt IPv4/IPv6 automatisch (`ip_family: auto`), installiert k3s als Server auf dem
Master und als Agent auf den Workern, und liefert eine fertig adressierte Kubeconfig. Zwei
Standardeinstellungen wurden bewusst überschrieben: **Longhorn** (repliziertes Storage,
automatisch an bei drei Knoten) ist aus — bei 124 MB Tagesvolumen unnötiger Ausfallpunkt — und
**automatische nächtliche k3s-Updates** sind aus, damit kein Cluster-Neustart mitten in die
Projektwoche fällt.

**4. Anwendung deployen.** Siehe "Ein Chart, zwei Umgebungen" oben.

### Abbau

```bash
cd terraform && terraform destroy
```

Erst **nach** den Screenshots. Die VMs belegen bis dahin Kontingent, das sich der ganze Kurs teilt.

---

## 10. Wesentliche Codeabschnitte

Verlinkte Dateien mit je einem Satz, warum die Stelle wesentlich ist — kein „hier wird X
gemacht", sondern was daran eine Entscheidung oder ein Verständnis zeigt.

| Datei | Warum wesentlich |
|---|---|
| `ingestion/schema/unified_schema.py` | die Normalisierung dreier Rohformate — der fachliche Kern |
| `ingestion/producer/kafka_producer.py` | `machine_id`/`MACHINE_PARTITION` als Partition-Key, sichert Reihenfolge je Maschine |
| `stream-processing/streaming_job.py` | Fenster, Watermark, Partitionierung (`machine_type`, `event_date`)|
| `serving-api/app/storage.py` | TTL-Cache gegen wiederholte Full-Scans durch Readiness-Probes, `load_table()` |
| `ui/data_source.py` | Anbindung an die Serving-API statt Mock, kurzer serverseitiger Cache gegen Mehrfachanfragen |
| `charts/mes-pipeline/templates/kafka.yaml` | `podManagementPolicy: Parallel` + `publishNotReadyAddresses` — die beiden Bootstrapping-Deadlocks beim 3-Broker-Umbau |
| `charts/mes-pipeline/templates/serving-api.yaml` | HPA ohne von Helm verwaltete `replicas` — kein Kampf zwischen `helm upgrade` und der Autoskalierung |

---


## 12. Grenzen und Ausblick

Dieser Abschnitt benennt bewusst, was der Prototyp **nicht** leistet. Eine ehrlich benannte
Lücke kostet weniger als eine Behauptung, die im Gespräch nicht trägt.

### Datenvolumen

124 MB am Tag sind kein Big Data. Die Architektur ist auf ein Vielfaches ausgelegt, der
Prototyp erzeugt es nicht. Die Simulatoren verwenden zudem nur drei verschiedene `machine_id` —
für eine realistische Partitionsverteilung wären mehr nötig.

### Skalierbarkeit der Verarbeitung

Der Streaming-Job läuft mit `.master("local[2]")` fest verdrahtet auf zwei Threads in einem
einzigen Pod. Das erfüllt die Anforderung „Skalierbarkeit in allen Komponenten" an dieser Stelle
**nicht**, und das ist eine Einschränkung, keine Designentscheidung: Zwei Replicas wären zwei
unabhängige Spark-Anwendungen, die dasselbe Topic vom selben Offset lesen und dieselben
Aggregate doppelt schreiben würden.


### Bronze-Schicht

Die Rohereignisse liegen ausschließlich im Kafka-Log mit 7 Tagen Retention. Danach ist die
Rohhistorie unwiederbringlich weg. Ein Produktivbetrieb bräuchte eine zusätzliche Ablage.

### Kein Katalog, kein Table Format

Ohne Delta oder Iceberg gibt es keine ACID-Transaktionen, kein Time Travel und keine
Schema-Evolution. Ein gleichzeitiger Schreib- und Lesevorgang auf dieselbe Partition kann eine
inkonsistente Sicht liefern. Für den Prototyp vertretbar, für Produktivbetrieb nicht.

### Betrieb

Kein zentrales Logging, keine Metriken, kein Alerting. Fehler werden über `kubectl logs`
gefunden. Ein Observability-Stack wäre der nächste Schritt.

### Was als Nächstes käme

1. Materialisierte Gold-Schicht mit MES-Kennzahlen (OEE, Verfügbarkeit je Schicht)
2. Delta Lake statt reinem Parquet für ACID und Schema-Evolution
3. Spark-on-Kubernetes für horizontal skalierende Verarbeitung
4. Observability-Stack
5. Automatisierte Tests der Normalisierung
6. Geteilter Redis-Cache statt In-Memory-Cache pro Serving-API-Pod (siehe Guide 17) — geplant
   zwischen Freitags-Meeting und Abgabe, noch offen zum Zeitpunkt dieser Vorschau
