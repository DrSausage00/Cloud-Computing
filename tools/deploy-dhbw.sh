#!/bin/bash
cd ~/Cloud-Computing
git fetch --quiet origin main
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" != "$REMOTE" ]; then
  echo "$(date): neue Commits gefunden, deploye..." >> ~/deploy.log
  git pull --quiet origin main
  helm upgrade --install mes ./charts/mes-pipeline \
    --namespace mes --create-namespace \
    -f values-secret.yaml \
    -f ./charts/mes-pipeline/values-dhbw.yaml \
    --wait --timeout 10m >> ~/deploy.log 2>&1
  echo "$(date): deploy fertig, exit=$?" >> ~/deploy.log
fi

exit 0