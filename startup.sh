#!/usr/bin/env bash
set -euo pipefail

NETWORK="demonstrator"

build_if_missing() {
  local image="$1" context="$2"
  if docker image inspect "$image" >/dev/null 2>&1; then
    echo "Image '$image' exists, skipping build"
  else
    echo "Image '$image' not found, building from $context..."
    docker build -t "$image" "$context"
  fi
}

build_if_missing im-simulator:latest ./demonstrator/im_simulator
build_if_missing gateway:latest      ./demonstrator/gateway
build_if_missing opcua-client:latest ./demonstrator/opc_ua_client

if docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Network '$NETWORK' already exists"
else
  docker network create "$NETWORK"
  echo "Network '$NETWORK' created"
fi

echo "::: Starting core stack :::"
if ! docker compose -p core -f compose-core.yml up -d --wait; then
  echo "Core stack failed to start - not starting demonstrator stack" >&2
  exit 1
fi
echo "Core stack is up"

echo "::: Starting demonstrator stack :::"
docker compose -p demonstrator -f compose-demonstrator.yml up -d
echo "Demonstrator stack is up"
