#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v --remove-orphans
docker compose config --images | xargs -r docker rmi -f 2>/dev/null || true

echo "Stack fully removed."
