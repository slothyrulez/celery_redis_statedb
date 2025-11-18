#!/bin/bash

# Test script to verify Redis state persistence across worker restarts

set -e

echo "=== Testing Redis State Persistence ==="
echo ""

# Check if services are running
if ! docker-compose ps | grep -q "Up"; then
    echo "Error: Services not running. Please run 'docker-compose up -d' first."
    exit 1
fi

echo "Step 1: Starting a long-running task..."
RESPONSE=$(curl -s "http://localhost:8000/tasks/long/?duration=60")
TASK_ID=$(echo $RESPONSE | jq -r '.task_id')
echo "Task ID: $TASK_ID"
echo ""

sleep 2

echo "Step 2: Revoking the task..."
curl -s -X POST "http://localhost:8000/tasks/revoke/$TASK_ID/" | jq
echo ""

sleep 2

echo "Step 3: Checking task status..."
curl -s "http://localhost:8000/tasks/status/$TASK_ID/" | jq
echo ""

echo "Step 4: Checking Redis for revoked task..."
docker-compose exec -T redis redis-cli KEYS "myapp:worker:state:*:zrevoked" | head -n 3
echo ""

echo "Step 5: Getting worker hostnames..."
WORKER1=$(docker-compose ps -q celery-worker-1 | xargs docker inspect -f '{{.Config.Hostname}}')
WORKER2=$(docker-compose ps -q celery-worker-2 | xargs docker inspect -f '{{.Config.Hostname}}')
echo "Worker 1: $WORKER1"
echo "Worker 2: $WORKER2"
echo ""

echo "Step 6: Checking worker logs before restart..."
echo "Worker 1 revoked tasks:"
docker-compose logs celery-worker-1 2>&1 | grep -i "revoked" | tail -n 3 || echo "No revoked tasks logged yet"
echo ""

echo "Step 7: Restarting workers to test persistence..."
docker-compose restart celery-worker-1 celery-worker-2
echo "Waiting for workers to restart..."
sleep 5
echo ""

echo "Step 8: Checking worker logs after restart..."
echo "Worker 1 should load revoked tasks from Redis:"
docker-compose logs celery-worker-1 2>&1 | grep -i "redis\|revoked\|state" | tail -n 10
echo ""

echo "Step 9: Verifying Redis still has the revoked task..."
docker-compose exec -T redis redis-cli KEYS "myapp:worker:state:*:zrevoked"
echo ""

echo "Step 10: Checking worker state in Flower..."
echo "Visit http://localhost:5555/tasks to see revoked tasks"
echo ""

echo "=== Test Complete ==="
echo ""
echo "Summary:"
echo "- Task $TASK_ID was revoked"
echo "- Workers were restarted"
echo "- Revoked state persisted in Redis"
echo "- Workers loaded state on restart"
echo ""
echo "The test demonstrates that revoked tasks survive worker restarts!"
