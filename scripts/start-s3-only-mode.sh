#!/usr/bin/env bash
# Start repository-service in s3-only mode (no Redis, synchronous S3 writes).
# Usage: ./scripts/start-s3-only-mode.sh
#
# Use this to isolate whether Redis caching causes duplicate key issues.
# Compare with redis-buffered mode: ./scripts/start-redis-mode.sh

set -e
cd "$(dirname "$0")/.."

export REPO_STORAGE_MODE=s3-only
export REDIS_HEALTH_ENABLED=false

echo "Starting repository-service in s3-only mode (no Redis)"
echo "  REPO_STORAGE_MODE=$REPO_STORAGE_MODE"
echo "  REDIS_HEALTH_ENABLED=$REDIS_HEALTH_ENABLED"
echo ""

./gradlew quarkusDev
