#!/usr/bin/env bash
# Usage: scripts/ladder.sh 1000 1500 2000     (JWT_SECRET must be exported)
set -euo pipefail

lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:19092 --describe --group fraud-processor 2>/dev/null \
    | awk '$2=="transactions" {s+=$6} END {print s+0}'
}

for rate in "$@"; do
  echo "=== waiting for consumer lag to drain ==="
  until [ "$(lag)" = "0" ]; do sleep 2; done
  docker compose exec -T redis redis-cli flushall >/dev/null
  echo "=== ${rate}/s ==="
  go run ./cmd/loadgen -rate "$rate" -duration 30s -users 100000
done