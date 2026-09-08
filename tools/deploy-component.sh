#!/usr/bin/env bash
set -euo pipefail
name="$1"
tag="${2:-0.1}"
docker build -t "mes/$name:$tag" "./$name"
minikube image load "mes/$name:$tag"
kubectl rollout restart "deployment/$name" -n mes
kubectl rollout status "deployment/$name" -n mes
