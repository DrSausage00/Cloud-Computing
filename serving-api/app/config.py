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

# Timeouts gegen MinIO:
# Ohne diese Timeouts kann die API hängen bleiben, wenn MinIO nicht antwortet.
# Das blockiert Uvicorn-Worker -> API reagiert nicht mehr (kritischer Bug).
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    "config_kwargs": {
        "connect_timeout": 5,   # Verbindung muss innerhalb 5s stehen
        "read_timeout": 10,     # Lesen muss innerhalb 10s erfolgen
    },
}