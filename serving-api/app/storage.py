"""
Datenzugriff: liest die Parquet-Dateien, die Spark nach MinIO schreibt,
in einen pandas-DataFrame ein.
"""

import pickle
import time

import pandas as pd

from .config import (
    REDIS_HOST,
    REDIS_PORT,
    S3_CACHE_TTL_SECONDS,
    SILVER_PATH,
    STORAGE_OPTIONS,
)

_cache = {"df": None, "ts": 0.0}
_REDIS_CACHE_KEY = "silver_table"

_redis = None
if REDIS_HOST:
    import redis

    _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def _load_from_source() -> pd.DataFrame:
    """
    Liest die Silver-Schicht tatsaechlich aus MinIO (ohne jegliches Caching).

    Wichtige Aspekte:
    -----------------
    1. RETRY-MECHANISMUS
       Spark schreibt laufend neue Dateien in das Verzeichnis.
       Pandas/pyarrow listet zuerst alle Dateien auf und öffnet sie dann.
       Wenn Spark eine Datei zwischen diesen beiden Schritten ersetzt,
       entsteht ein FileNotFoundError -> transienter Fehler.

       Lösung: 3 Versuche mit kurzer Pause.

    2. TIMEOUTS
       Wenn MinIO hängt, darf die API nicht blockieren.
       Die Timeouts (siehe config.py) sorgen dafür, dass die API
       nach spätestens 10s mit einem Fehler reagiert statt dauerhaft
       zu hängen.

    3. ZEITSTEMPEL-KONVERTIERUNG
       window_start/window_end müssen echte UTC-Timestamps sein,
       damit Filterung und Sortierung korrekt funktionieren.
    """
    last_exc = None

    for _attempt in range(3):
        try:
            df = pd.read_parquet(SILVER_PATH, storage_options=STORAGE_OPTIONS)

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


def load_table() -> pd.DataFrame:
    """
    Liest die gesamte Silver-Schicht als DataFrame ein (mit Cache).

    Nutzt Redis als geteilten Cache, falls REDIS_HOST konfiguriert ist,
    sonst einen In-Memory-Cache pro Prozess (siehe Modul-Docstring oben).
    """
    if _redis is not None:
        try:
            cached = _redis.get(_REDIS_CACHE_KEY)
            if cached is not None:
                return pickle.loads(cached)
        except redis.RedisError:
            # Redis nicht erreichbar -> nicht crashen, sondern pruefen, ob
            # der lokale In-Memory-Cache dieses Pods noch gueltig ist,
            # bevor ein neuer Vollscan noetig wird.
            now = time.time()
            if _cache["df"] is not None and now - _cache["ts"] < S3_CACHE_TTL_SECONDS:
                return _cache["df"]

        df = _load_from_source()

        try:
            _redis.setex(_REDIS_CACHE_KEY, S3_CACHE_TTL_SECONDS, pickle.dumps(df))
        except redis.RedisError:
            # Schreiben nach Redis optional: das frisch gelesene Ergebnis
            # bleibt trotzdem gueltig. Zusaetzlich lokal cachen, damit
            # Folgeaufrufe waehrend des Ausfalls nicht erneut einen
            # Vollscan ausloesen.
            _cache["df"] = df
            _cache["ts"] = time.time()

        return df

    now = time.time()
    if _cache["df"] is not None and now - _cache["ts"] < S3_CACHE_TTL_SECONDS:
        return _cache["df"]

    df = _load_from_source()

    _cache["df"] = df
    # WICHTIG: ts wird bewusst NACH dem Read (_load_from_source) gesetzt,
    # nicht mit dem `now` von oben. Waere ts = now, wuerde bei einem Read,
    # der laenger als die TTL dauert (beobachtet: 15-20s bei einer TTL von
    # 5s), der Cache-Eintrag bereits im Moment des Schreibens als
    # abgelaufen gelten - der Cache haette dann nie gegriffen, obwohl er
    # syntaktisch korrekt aussah.
    _cache["ts"] = time.time()
    return df
