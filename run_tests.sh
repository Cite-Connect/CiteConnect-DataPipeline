#!/bin/bash
# Run all tests with coverage

echo "Running tests with coverage..."
pytest tests/ \
  --cov=preprocessing \
  --cov=embeddings \
  --cov=services \
  --cov=utils \
  --cov-report=html \
  --cov-report=term \
  -v

echo ""
echo "Coverage report generated in htmlcov/index.html"
