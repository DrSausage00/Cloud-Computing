"""
Datenzugriff: liest die Parquet-Dateien, die Spark nach MinIO schreibt,
in einen pandas-DataFrame ein.
"""

import time

import pandas as pd

from .config import SILVER_PATH, STORAGE_OPTIONS


def load_table() -> pd.DataFrame:
    """
    Liest die gesamte Silver-Schicht als DataFrame ein.

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
