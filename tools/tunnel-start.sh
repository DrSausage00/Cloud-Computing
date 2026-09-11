#!/bin/bash
LOGFILE=/tmp/cloudflared.log
REPO=~/Cloud-Computing
URLFILE="$REPO/docs/current-tunnel-url.txt"

pkill -f "cloudflared tunnel" 2>/dev/null
sleep 1

nohup cloudflared tunnel --url "http://[fd00:43::4da9]:8050" > "$LOGFILE" 2>&1 &

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

echo "Tunnel-URL: $URL"

mkdir -p "$REPO/docs"
{
  echo "# Aktuelle Cloudflare-Tunnel-URL"
  echo ""
  echo "Automatisch generiert von tools/tunnel-start.sh auf mes-master."
  echo "Stand: $(date)"
  echo ""
  echo "$URL"
} > "$URLFILE"

cd "$REPO"
git add "$URLFILE"
git commit -m "chore: Tunnel-URL aktualisiert ($(date +%Y-%m-%d_%H:%M))" --quiet
git push --quiet

echo "Fertig, URL in $URLFILE committed und gepusht."
