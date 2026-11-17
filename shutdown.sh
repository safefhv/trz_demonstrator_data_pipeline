#!/usr/bin/env bash
set -uo pipefail

status=0

echo "::: Stopping demonstrator stack :::"
if ! docker compose -p demonstrator -f compose-demonstrator.yml down; then
  echo "Failed to stop demonstrator stack" >&2
  status=1
fi

echo "::: Stopping core stack :::"
if ! docker compose -p core -f compose-core.yml down; then
  echo "Failed to stop core stack" >&2
  status=1
fi

exit "$status"
