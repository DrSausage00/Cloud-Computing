"""
Datenzugriff: liest die Parquet-Dateien, die Spark nach MinIO schreibt,
in einen pandas-DataFrame ein.
"""

import pickle
import time
from datetime import date
from typing import Optional

import pandas as pd

from .config import (
    REDIS_HOST,
    REDIS_PORT,
    S3_CACHE_TTL_SECONDS,
    SILVER_PATH,
    STATUS_PATH,
    STORAGE_OPTIONS,
)

# Cache ist jetzt pro Filterwert (event_date_filter) getrennt, weil
# /metrics/history und /metrics/latest unterschiedliche Partitions-Fenster
# anfragen (siehe load_table()). Ein einzelner globaler Eintrag wuerde sonst
# Ergebnisse fuer unterschiedliche Cutoff-Daten ueberschreiben.
_cache: dict[str, dict] = {}
_REDIS_CACHE_KEY_PREFIX = "silver_table"

_redis = None
if REDIS_HOST:
    import redis

    _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def _cache_key(event_date_filter: Optional[date]) -> str:
    return event_date_filter.isoformat() if event_date_filter else "all"


def _load_from_source(event_date_filter: Optional[date] = None) -> pd.DataFrame:
    """
    Liest die Silver-Schicht tatsaechlich aus MinIO (ohne jegliches Caching).

    Wichtige Aspekte:
    -----------------
    1. PARTITION PRUNING
       Silver ist nach `machine_type`/`event_date` partitioniert. Wird
       `event_date_filter` gesetzt, uebergeben wir ihn als pyarrow-`filters`
       an pd.read_parquet(). Damit werden irrelevante Partitions-Ordner gar
       nicht erst aufgelistet/gelesen (nicht: alles laden und danach in
       Pandas verwerfen). Das ist der eigentliche Hebel gegen die hohe
       CPU/RAM-Last bei mittlerweile zehntausenden kleinen Dateien.

    2. RETRY-MECHANISMUS
       Spark schreibt laufend neue Dateien in das Verzeichnis.
       Pandas/pyarrow listet zuerst alle Dateien auf und öffnet sie dann.
       Wenn Spark eine Datei zwischen diesen beiden Schritten ersetzt,
       entsteht ein FileNotFoundError -> transienter Fehler.

       Lösung: 3 Versuche mit kurzer Pause.

    3. TIMEOUTS
       Wenn MinIO hängt, darf die API nicht blockieren.
       Die Timeouts (siehe config.py) sorgen dafür, dass die API
       nach spätestens 10s mit einem Fehler reagiert statt dauerhaft
       zu hängen.

    4. ZEITSTEMPEL-KONVERTIERUNG
       window_start/window_end müssen echte UTC-Timestamps sein,
       damit Filterung und Sortierung korrekt funktionieren.
    """
    last_exc = None

    filters = (
        [("event_date", ">=", event_date_filter.isoformat())] if event_date_filter else None
    )

    for _attempt in range(3):
        try:
            df = pd.read_parquet(
                SILVER_PATH,
                storage_options=STORAGE_OPTIONS,
                filters=filters,
            )

            # Konvertierung der Zeitspalten
            df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
            df["window_end"] = pd.to_datetime(df["window_end"], utc=True)

            return df

        except FileNotFoundError as exc:
            # Transienter Fehler -> kurz warten und erneut versuchen
            last_exc = exc
            time.sleep(0.5)

        except Exception as exc:
            # Andere Fehler sofort weitergeben (z. B. Authentifizierung)
            raise exc

    # Wenn alle Versuche scheitern -> letzten Fehler weitergeben
    raise last_exc


def load_table(event_date_filter: Optional[date] = None) -> pd.DataFrame:
    """
    Liest die Silver-Schicht als DataFrame ein (mit Cache).

    `event_date_filter` grenzt via Partition Pruning auf `event_date >=
    event_date_filter` ein (siehe _load_from_source()). Wird nichts
    uebergeben, wird weiterhin die komplette Historie gelesen -
    Aufrufer sollten das i.d.R. vermeiden.

    Nutzt Redis als geteilten Cache, falls REDIS_HOST konfiguriert ist,
    sonst einen In-Memory-Cache pro Prozess (siehe Modul-Docstring oben).
    Der Cache ist pro `event_date_filter` getrennt (siehe _cache_key()).
    """
    cache_key = _cache_key(event_date_filter)
    redis_key = f"{_REDIS_CACHE_KEY_PREFIX}:{cache_key}"
    local_entry = _cache.get(cache_key, {"df": None, "ts": 0.0})

    if _redis is not None:
        try:
            cached = _redis.get(redis_key)
            if cached is not None:
                return pickle.loads(cached)
        except redis.RedisError:
            # Redis nicht erreichbar -> nicht crashen, sondern pruefen, ob
            # der lokale In-Memory-Cache dieses Pods noch gueltig ist,
            # bevor ein neuer Vollscan noetig wird.
            now = time.time()
            if (
                local_entry["df"] is not None
                and now - local_entry["ts"] < S3_CACHE_TTL_SECONDS
            ):
                return local_entry["df"]

        df = _load_from_source(event_date_filter)

        try:
            _redis.setex(redis_key, S3_CACHE_TTL_SECONDS, pickle.dumps(df))
        except redis.RedisError:
            # Schreiben nach Redis optional: das frisch gelesene Ergebnis
            # bleibt trotzdem gueltig. Zusaetzlich lokal cachen, damit
            # Folgeaufrufe waehrend des Ausfalls nicht erneut einen
            # Vollscan ausloesen.
            _cache[cache_key] = {"df": df, "ts": time.time()}

        return df

    now = time.time()
    if (
        local_entry["df"] is not None
        and now - local_entry["ts"] < S3_CACHE_TTL_SECONDS
    ):
        return local_entry["df"]

    df = _load_from_source(event_date_filter)

    # WICHTIG: ts wird bewusst NACH dem Read (_load_from_source) gesetzt,
    # nicht mit dem `now` von oben. Waere ts = now, wuerde bei einem Read,
    # der laenger als die TTL dauert (beobachtet: 15-20s bei einer TTL von
    # 5s), der Cache-Eintrag bereits im Moment des Schreibens als
    # abgelaufen gelten - der Cache haette dann nie gegriffen, obwohl er
    # syntaktisch korrekt aussah.
    _cache[cache_key] = {"df": df, "ts": time.time()}
    return df


def load_status() -> pd.DataFrame:
    return pd.read_parquet(STATUS_PATH, storage_options=STORAGE_OPTIONS)