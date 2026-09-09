"""
MES Serving-API

1. Liest die Parquet-Dateien, die Spark nach MinIO schreibt, in eine Tabelle ein.
2. Bietet zwei Endpunkte, die genau das liefern, was die UI erwartet.

Starten (aus dem Projekt-Wurzelverzeichnis):
    uvicorn app.main:app --reload --port 8000

Testen im Browser:
    http://localhost:8000/docs
"""

from fastapi import FastAPI

from app.routers import health, metrics

app = FastAPI(title="MES Serving API")

app.include_router(health.router)
app.include_router(metrics.router)
