#!/bin/bash
LOGFILE=/tmp/cloudflared-minio.log
GIST_ID="79c6ec67c2fd4b6d71eb85fb5d5ca439"
TOKEN_FILE=~/github-gist-token.txt

pkill -f "cloudflared tunnel --url.*9001" 2>/dev/null
sleep 1

nohup cloudflared tunnel --url "http://[fd00:43::f7eb]:9001" > "$LOGFILE" 2>&1 &

echo "Warte auf Tunnel-URL..."
URL=""
for i in $(seq 1 30); do
  URL=$(grep -oE "https://[a-zA-Z0-9.-]+\.trycloudflare\.com" "$LOGFILE" | head -1)
  if [ -n "$URL" ]; then
    break
  fi
  sleep 1
done

if [ -z "$URL" ]; then
  echo "Konnte keine Tunnel-URL finden, siehe $LOGFILE"
  exit 1
fi

echo "MinIO-Console-URL: $URL"

TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')
CONTENT="Aktuelle MinIO-Console-URL (Stand: $(date)):\n\n${URL}\n\nLogin: siehe values-secret.yaml (rootUser/rootPassword)"

curl -s -X PATCH "https://api.github.com/gists/$GIST_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"files\":{\"minio-console-url.txt\":{\"content\":\"$CONTENT\"}}}" > /dev/null

echo "Fertig, Gist aktualisiert."
