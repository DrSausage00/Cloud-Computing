"""
Datenzugriff: liest die Parquet-Dateien, die Spark nach MinIO schreibt,
in einen pandas-DataFrame ein.
"""

import time

import pandas as pd

from .config import SILVER_PATH, STORAGE_OPTIONS

# Readiness-Probe löst alle 10–15s einen kompletten Silver-Scan aus; bei vielen Dateien wird das zu langsam.
# Ein kleiner Cache (TTL ~5s) verringert die Leserate und verhindert unnötige Last sowie HPA‑Überreaktionen.
_CACHE_TTL_SECONDS = 5
_cache = {"df": None, "ts": 0.0}

def load_table() -> pd.DataFrame:
    """
    Liest die gesamte Silver-Schicht als DataFrame ein (mit kurzlebigem Cache).

    Wichtige Aspekte:
    -----------------
    1. CACHE
       Siehe _CACHE_TTL_SECONDS oben: haeufige /ready-Probes und /metrics-
       Aufrufe innerhalb weniger Sekunden teilen sich denselben gelesenen
       DataFrame, statt jeweils einen frischen Vollscan zu erzwingen.

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

    now = time.time()
    if _cache["df"] is not None and now - _cache["ts"] < _CACHE_TTL_SECONDS:
        return _cache["df"]

    last_exc = None

    for _attempt in range(3):
        try:
            df = pd.read_parquet(SILVER_PATH, storage_options=STORAGE_OPTIONS)

            # Konvertierung der Zeitspalten
            df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
            df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
            
            _cache["df"] = df
            _cache["ts"] = now
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
