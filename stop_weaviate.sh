#!/bin/bash
# Stop Weaviate container

echo "Stopping Weaviate..."
docker stop weaviate
docker rm weaviate
echo "✅ Weaviate stopped"
