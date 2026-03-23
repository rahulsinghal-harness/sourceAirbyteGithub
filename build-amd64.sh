#!/usr/bin/env bash
# Build image; use linux/amd64 (x86_64) when Docker buildx is available.
set -e
TAG="${1:-0.0.14}"
IMAGE="rahulsinghalharness/sourceairbytegithub"

if docker buildx version &>/dev/null; then
  echo "Building for linux/amd64 (buildx found)..."
  docker buildx build --platform linux/amd64 -t "${IMAGE}:${TAG}" -t "${IMAGE}:latest" --load .
  echo "Built ${IMAGE}:${TAG} (linux/amd64). Pushing..."
else
  echo "Buildx not found. Building for host architecture only."
  echo "For x86_64 image: install buildx (Docker Desktop or https://docs.docker.com/build/install-buildx/) or build on an x86_64 machine."
  docker build -t "${IMAGE}:${TAG}" -t "${IMAGE}:latest" .
  echo "Built ${IMAGE}:${TAG} (host arch). Pushing..."
fi

docker push "${IMAGE}:${TAG}"
docker push "${IMAGE}:latest"
echo "Pushed ${IMAGE}:${TAG} and ${IMAGE}:latest"
