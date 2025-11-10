#!/bin/bash
# Stop old containers
docker stop weaviate t2v-transformers 2>/dev/null || true
docker rm weaviate t2v-transformers 2>/dev/null || true

# Start with Docker Compose
docker compose up -d

# Wait for startup (first time downloads model ~400MB)
echo "⏳ Waiting for Weaviate to initialize (this may take 2-3 minutes on first run)..."
sleep 60

# Check status
docker-compose ps

# Test connection
curl http://localhost:8080/v1/meta