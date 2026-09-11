#!/bin/bash
LOGFILE=/tmp/cloudflared-registry.log
TOKEN_FILE=~/github-gist-token.txt
REPO="DrSausage00/Cloud-Computing"

pkill -f "cloudflared tunnel --url.*5000" 2>/dev/null
sleep 1

nohup cloudflared tunnel --url "http://[fd00:43::738e]:5000" > "$LOGFILE" 2>&1 &

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

HOSTNAME=$(echo "$URL" | sed 's#https://##')
echo "Registry-Tunnel-URL: $URL (Host: $HOSTNAME)"

TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')

curl -s -X PATCH "https://api.github.com/repos/$REPO/actions/variables/REGISTRY_HOST" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"REGISTRY_HOST\",\"value\":\"$HOSTNAME\"}"

echo ""
echo "Fertig, REGISTRY_HOST Variable aktualisiert auf: $HOSTNAME"
