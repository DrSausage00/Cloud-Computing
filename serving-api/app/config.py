"""
Zentrale Konfiguration der MES Serving-API.

Alle Zugangsdaten und Pfade werden über Umgebungsvariablen gesteuert,
damit die API in verschiedenen Umgebungen (lokal, Podman, Kubernetes)
ohne Codeänderung funktioniert.
"""

import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_DATA_BUCKET = os.getenv("MINIO_DATA_BUCKET", "mes-data")

# Genau der Pfad, unter dem der Spark-Job schreibt (silver_path in streaming_job.py)
SILVER_PATH = f"s3://{MINIO_DATA_BUCKET}/silver/machine-metrics/"

# Feinabstimmung der S3-Timeouts/Retries ueber Env-Variablen, damit sie sich
# ueber die ConfigMap anpassen lassen, ohne Zugangsdaten/Endpoint anzufassen
# und ohne dafuer den Code zu aendern. Defaults entsprechen den bisherigen
# fest codierten Werten.
S3_CONNECT_TIMEOUT = int(os.getenv("S3_CONNECT_TIMEOUT", "5"))
S3_READ_TIMEOUT = int(os.getenv("S3_READ_TIMEOUT", "10"))
S3_RETRY_MAX_ATTEMPTS = int(os.getenv("S3_RETRY_MAX_ATTEMPTS", "1"))
S3_USE_LISTINGS_CACHE = os.getenv("S3_USE_LISTINGS_CACHE", "false").lower() == "true"

# Timeouts gegen MinIO:
# Ohne diese Timeouts kann die API hängen bleiben, wenn MinIO nicht antwortet.
# Das blockiert Uvicorn-Worker -> API reagiert nicht mehr (kritischer Bug).
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    "config_kwargs": {
        "connect_timeout": S3_CONNECT_TIMEOUT,   # Verbindung muss innerhalb 5s stehen
        "read_timeout": S3_READ_TIMEOUT,     # Lesen muss innerhalb 10s erfolgen
        "retries": {"mode": "standard", "max_attempts": S3_RETRY_MAX_ATTEMPTS},
    },
    "use_listings_cache": S3_USE_LISTINGS_CACHE,
}