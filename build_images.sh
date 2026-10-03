#!/bin/bash
set -e

TAG="${1:-v1}"
CONTAINERS_DIR="$(cd "$(dirname "$0")" && pwd)/containers"

IMAGES=(scraper cleaner embedder bias-classifier-xgb bias-classifier-transformers clustering loader)

for name in "${IMAGES[@]}"; do
  echo "Building newslens/$name:$TAG ..."
  docker build -t "newslens/$name:$TAG" "$CONTAINERS_DIR/$name/"
  echo ""
done

echo "All images built:"
docker images | grep newslens
