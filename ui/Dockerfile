FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8050

# Gunicorn statt Dash-Entwicklungsserver: mehrere Worker pro Pod,
# stabil unter Last. "app:server" ist das Flask-Objekt aus app.py.
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "app:server"]
