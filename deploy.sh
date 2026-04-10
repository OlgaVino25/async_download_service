#!/bin/bash
set -e

docker build -t archive-service .

docker stop archive-service || true
docker rm archive-service || true

docker run -d \
  --name archive-service \
  -p 8080:8080 \
  -v /opt/photos:/app/photos \
  -e PHOTOS_DIR=/app/photos \
  -e LOG_LEVEL=WARNING \
  archive-service

echo "Deployed successfully"