"""
Health- und Readiness-Endpunkte für Kubernetes.
"""

from fastapi import APIRouter, HTTPException

from ..storage import load_table
from ..storage import load_status

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """
    Liveness-Probe:
    ----------------
    Zeigt nur an, dass der Prozess läuft und Anfragen beantwortet.
    Sagt NICHTS über MinIO oder Datenqualität aus.
    """
    return {"status": "ok"}


@router.get("/ready")
def ready():
    """
    Readiness-Probe:
    ----------------
    Prüft, ob die API wirklich Daten liefern kann:
    - MinIO erreichbar?
    - Parquet-Dateien lesbar?
    - Tabelle nicht korrupt?

    Kubernetes nutzt diesen Endpunkt, um Pods erst dann in den
    Load-Balancer aufzunehmen, wenn sie wirklich bereit sind.
    """
    try:
        df = load_status()
        return {"status": "ok", "rows": len(df)}
    except Exception as exc:
        raise HTTPException(
            status_code=503, detail=f"Datenquelle nicht erreichbar: {exc}"
        )
