#!/bin/bash
LOGFILE=/tmp/cloudflared-k8sapi.log
TOKEN_FILE=~/github-gist-token.txt
REPO="DrSausage00/Cloud-Computing"

pkill -f "cloudflared tunnel --url.*6443" 2>/dev/null
sleep 1

nohup cloudflared tunnel --url https://localhost:6443 --no-tls-verify > "$LOGFILE" 2>&1 &

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
echo "K8s-API-Tunnel-URL: $URL"

TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')

curl -s -X PATCH "https://api.github.com/repos/$REPO/actions/variables/K8S_API_HOST" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"K8S_API_HOST\",\"value\":\"$HOSTNAME\"}"

echo ""
echo "Fertig, K8S_API_HOST aktualisiert auf: $HOSTNAME"
