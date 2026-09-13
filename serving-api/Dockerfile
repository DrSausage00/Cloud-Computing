# Basis-Image: schlankes Python 3.12
# Ideal für FastAPI + Pandas ohne unnötige Abhängigkeiten
FROM python:3.12-slim

# Arbeitsverzeichnis im Container
WORKDIR /app

# Requirements zuerst kopieren → ermöglicht Docker-Layer-Caching
COPY requirements.txt .

# Installation der Python-Abhängigkeiten
# --no-cache-dir verhindert unnötige Layer-Größe
RUN pip install --no-cache-dir -r requirements.txt

# Restlichen Code kopieren (main.py, etc.)
COPY . .

# Startbefehl für FastAPI/Uvicorn
# Host 0.0.0.0 → Container ist von außen erreichbar
# app.main:app → main.py liegt jetzt im app/-Package, nicht mehr im Root
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "1", "-b", "[::]:8000", "--timeout", "120", "app.main:app"]

