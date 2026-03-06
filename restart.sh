#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v --remove-orphans
docker compose up -d --build

echo ""
echo "Stack restarting. Services will be ready in ~30-60s."
echo "Monitor with: docker compose logs -f --tail 100"
