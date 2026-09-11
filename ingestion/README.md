# Ingestion

Simulation der Maschinendaten, Normalisierung der drei Rohformate auf das einheitliche
Schema und Versand nach Kafka (Topic `machine-events`).

## Datenfluss

```text
Simulator (A/B/C)  ->  Parser  ->  MachineEvent  ->  Kafka-Producer  ->  machine-events
```

| Schritt | Ordner | Aufgabe |
|---|---|---|
| Simulatoren | [`simulators/`](simulators/) | erzeugen Rohdaten im jeweiligen Originalformat |
| Parser | [`parsers/`](parsers/) | normalisieren auf `MachineEvent` |
| Schema | [`schema/`](schema/) | Definition von `MachineEvent` |
| Producer | [`producer/`](producer/) | Serialisierung, Partitionszuordnung, Versand |

Der Takt liegt standardmäßig bei 2 Sekunden pro Durchlauf, pro Durchlauf entstehen 10 Events
vom Typ A, 5 vom Typ B und 3 vom Typ C. Welche Typen ein Prozess simuliert, steuert
`MACHINE_TYPES` (siehe [Konfiguration](#konfiguration)).

## Formate und Felder

| Typ | Rohformat | Felder in `measurements` |
|---|---|---|
| A | JSON | `temperature`, `pressure`, `rotation_speed`, `power_consumption` |
| B | JSON, abweichende Feldnamen (`ts`, `id`, `temp`) | `temperature`, `vibration` |
| C | Pipe-separiert | `temperature`, `status` |

### Warum nur Typ C einen `status` liefert

Das ist beabsichtigt und kein fehlendes Feld. Nur Maschinentyp C besitzt im Use Case
überhaupt ein Statuskonzept (`RUNNING`, `STOPPED`, `ERROR`); A und B melden reine Messwerte
ohne Betriebszustand. Das ist genau der Veracity-Aspekt, den der Prototyp zeigen soll:
nicht jedes Ereignis trägt jedes Feld, und die Verarbeitung muss damit umgehen können
(siehe [README](../README.md) §2 und [interface-contracts.md](../docs/interface-contracts.md)).

Folge stromabwärts: `last_status` ist für Maschinen vom Typ A und B dauerhaft `null`.
Die UI sollte dafür „kein Status" bzw. eine neutrale Anzeige zeigen — es ist kein Fehler
in der Ingestion und kein Datenverlust.

## Partitionierung

`producer/kafka_producer.py` weist jeder bekannten Maschine über `MACHINE_IDS` fest eine
Partition zu, statt die Verteilung dem Standard-Partitionierer zu überlassen.

Grund: Der Standard-Partitionierer hasht die `machine_id`. Bei nur drei verschiedenen
Schlüsseln (`A-001`, `B-001`, `C-001`) landen alle drei zufällig auf derselben Partition —
die effektive Parallelität im Topic wäre 1 statt 3, unabhängig von der Anzahl der
Consumer oder Spark-Executors. Die feste Zuordnung verteilt garantiert gleichmäßig und
hält weiterhin die Reihenfolge je Maschine ein, weil dieselbe Maschine immer in derselben
Partition landet.

Die Partitionsanzahl wird zur Laufzeit vom Broker gelesen, die Zuordnung passt sich also
an. Maschinen, die nicht in `MACHINE_IDS` stehen, verteilt weiterhin der
Standard-Partitionierer. Kommen neue Maschinen dazu, gehören sie in diese Liste.

## Batching

`send_event()` puffert nur; `flush_events()` überträgt gesammelt und wird einmal je
Durchlauf in [`main.py`](main.py) aufgerufen. Ein `flush()` nach jeder einzelnen Nachricht
würde das Producer-Batching von Kafka aushebeln.

## Konfiguration

| Variable | Standard | Bedeutung |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Adresse des Brokers |
| `MACHINE_TYPES` | `A,B,C` | Kommaliste der Typen, die dieser Prozess simuliert |
| `INGESTION_INTERVAL_SECONDS` | `2` | Takt eines Durchlaufs |
| `MACHINE_A_COUNT` | `10` | Events vom Typ A pro Durchlauf |
| `MACHINE_B_COUNT` | `5` | Events vom Typ B pro Durchlauf |
| `MACHINE_C_COUNT` | `3` | Events vom Typ C pro Durchlauf |

Ein unbekannter Typ oder eine leere Liste in `MACHINE_TYPES` beendet den Prozess beim Start
mit einer Fehlermeldung, statt still nichts zu produzieren.

[`.env.example`](.env.example) ist die Vorlage für die MinIO-Zugangsdaten, die
`docker-compose.yml` benötigt — nicht für die Ingestion selbst.

## Horizontale Skalierung

`MACHINE_TYPES` erlaubt es, denselben Container-Build mehrfach zu deployen und die
Maschinentypen auf mehrere Ingestion-Pods aufzuteilen:

| Pod | `MACHINE_TYPES` | Produziert nach Partition |
|---|---|---|
| `ingestion-a` | `A` | 0 (`A-001`) |
| `ingestion-b` | `B` | 1 (`B-001`) |
| `ingestion-c` | `C` | 2 (`C-001`) |

Das passt zur festen Partitionszuordnung oben: Jeder Pod schreibt in genau eine Partition,
die Reihenfolge je Maschine bleibt erhalten und das Topic wird tatsächlich parallel
beschrieben.

Wichtig: Ein Deployment einfach auf `replicas: 3` zu setzen skaliert **nicht** — jede Replica
würde denselben Typ mit demselben `machine_id` simulieren und Duplikate erzeugen. Die
Aufteilung muss über `MACHINE_TYPES` erfolgen, also über je ein Deployment pro Typ.

## Lokal starten

Zuerst die Infrastruktur (Kafka mit 3 Partitionen für `machine-events`, MinIO):

```bash
cp .env.example .env   # Passwörter setzen
docker compose up -d
```

Dann die Ingestion:

```bash
pip install -r requirements.txt
python main.py
```
