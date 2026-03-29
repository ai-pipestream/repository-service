#!/usr/bin/env bash
# Start repository-service in redis-buffered mode (Redis L1 cache + S3 L2).
# Usage: ./scripts/start-redis-mode.sh
#
# Requires Redis running (docker compose provides it on port 6379).
# Compare with default s3-only mode: ./gradlew quarkusDev

set -e
cd "$(dirname "$0")/.."

export REPO_STORAGE_MODE=redis-buffered
export REDIS_HEALTH_ENABLED=true

echo "Starting repository-service in redis-buffered mode"
echo "  REPO_STORAGE_MODE=$REPO_STORAGE_MODE"
echo "  REDIS_HEALTH_ENABLED=$REDIS_HEALTH_ENABLED"
echo ""

./gradlew quarkusDev
