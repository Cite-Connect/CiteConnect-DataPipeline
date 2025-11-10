#!/bin/bash

# =================================================================
# CiteConnect Embedding Service Setup Script
# Creates all required directories and placeholder files
# =================================================================

set -e  # Exit on error

echo "=================================================="
echo "   CiteConnect Embedding Service Setup"
echo "=================================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📁 Creating directory structure..."

# =================================================================
# 1. CREATE DIRECTORIES
# =================================================================

mkdir -p preprocessing
mkdir -p embeddings
mkdir -p services
mkdir -p tests
mkdir -p tests/fixtures
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p airflow/dags
mkdir -p graph
mkdir -p config

echo "✅ Directories created"

# =================================================================
# 2. CREATE PREPROCESSING MODULE FILES
# =================================================================

echo ""
echo "📝 Creating preprocessing module files..."

# preprocessing/__init__.py
cat > preprocessing/__init__.py << 'EOF'
"""
CiteConnect Preprocessing Module
Handles text cleaning, chunking, and metadata enrichment.
"""

from .text_cleaner import TextCleaner
from .chunker import DocumentChunker
from .metadata_enricher import MetadataEnricher

__all__ = ["TextCleaner", "DocumentChunker", "MetadataEnricher"]
EOF

# Create placeholder files
touch preprocessing/text_cleaner.py
touch preprocessing/chunker.py
touch preprocessing/metadata_enricher.py

echo "  ✅ preprocessing/text_cleaner.py"
echo "  ✅ preprocessing/chunker.py"
echo "  ✅ preprocessing/metadata_enricher.py"

# =================================================================
# 3. CREATE EMBEDDINGS MODULE FILES
# =================================================================

echo ""
echo "📝 Creating embeddings module files..."

# embeddings/__init__.py
cat > embeddings/__init__.py << 'EOF'
"""
CiteConnect Embeddings Module
Handles embedding generation and vector storage with Weaviate.
"""

from .config import EmbeddingConfig
from .embed_generator import EmbeddingGenerator
from .vector_store import WeaviateStore

__all__ = ["EmbeddingConfig", "EmbeddingGenerator", "WeaviateStore"]
EOF

touch embeddings/config.py
touch embeddings/embed_generator.py
touch embeddings/vector_store.py
touch embeddings/openai_embedder.py

echo "  ✅ embeddings/config.py"
echo "  ✅ embeddings/embed_generator.py"
echo "  ✅ embeddings/vector_store.py"
echo "  ✅ embeddings/openai_embedder.py"

# =================================================================
# 4. CREATE SERVICES FILES
# =================================================================

echo ""
echo "📝 Creating services files..."

touch services/__init__.py
touch services/embedding_service.py

echo "  ✅ services/embedding_service.py"

# =================================================================
# 5. CREATE UTILS FILES
# =================================================================

echo ""
echo "📝 Creating additional utils files..."

touch utils/__init__.py
touch utils/gcs_reader.py

echo "  ✅ utils/gcs_reader.py"

# =================================================================
# 6. CREATE TEST FILES
# =================================================================

echo ""
echo "📝 Creating test structure..."

# tests/__init__.py
touch tests/__init__.py

# conftest.py
cat > tests/conftest.py << 'EOF'
"""
Pytest configuration and shared fixtures.
"""
import pytest
import pandas as pd
from typing import Dict, List

# Shared fixtures will go here
EOF

touch tests/test_preprocessing.py
touch tests/test_embeddings.py
touch tests/test_integration.py
touch tests/test_gcs_reader.py

# Test fixtures
touch tests/fixtures/sample_papers.json
touch tests/fixtures/sample_paper.parquet

echo "  ✅ tests/conftest.py"
echo "  ✅ tests/test_preprocessing.py"
echo "  ✅ tests/test_embeddings.py"
echo "  ✅ tests/test_integration.py"

# =================================================================
# 7. CREATE CONFIGURATION FILES
# =================================================================

echo ""
echo "📝 Creating configuration files..."

# .env.example
cat > .env.example << 'EOF'
# ============================================
# GOOGLE CLOUD CONFIGURATION
# ============================================
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
GCS_BUCKET_NAME=citeconnect-processed-parquet

# ============================================
# EMBEDDING CONFIGURATION
# ============================================
# Provider: 'weaviate' (default) or 'openai' (fallback)
EMBEDDING_PROVIDER=weaviate

# Weaviate Settings
WEAVIATE_URL=http://localhost:8080
WEAVIATE_API_KEY=  # Optional, leave empty for local

# OpenAI Settings (for fallback provider)
OPENAI_API_KEY=sk-your-api-key-here
OPENAI_EMBEDDING_MODEL=text-embedding-3-small

# Semantic Scholar API (optional)
SEMANTIC_SCHOLAR_KEY=

# ============================================
# PROCESSING CONFIGURATION
# ============================================
EMBEDDING_DIM=768
EMBEDDING_BATCH_SIZE=100
CHUNK_SIZE=512
CHUNK_OVERLAP=50

# ============================================
# WEAVIATE CONFIGURATION
# ============================================
WEAVIATE_COLLECTION=PaperChunks
EOF

# requirements.txt
cat > requirements.txt << 'EOF'
# Core dependencies
requests>=2.31.0
pandas>=2.0.0
beautifulsoup4>=4.12.0
pymupdf>=1.23.0
pyarrow>=13.0.0
python-dotenv>=1.0.0

# Data ingestion
grobid-client-python>=0.8.0

# Google Cloud
google-cloud-storage>=2.10.0

# Embeddings & Vector Store
weaviate-client>=4.4.0
sentence-transformers>=2.2.0
torch>=2.0.0

# Optional: OpenAI fallback
openai>=1.0.0

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0
responses>=0.23.0
freezegun>=1.2.2
faker>=19.0.0

# Orchestration (optional for now)
# apache-airflow>=2.7.0

# Neo4j (for future citation graph)
# neo4j>=5.0.0

# PostgreSQL (for future metadata store)
# psycopg2-binary>=2.9.0
EOF

# requirements-dev.txt
cat > requirements-dev.txt << 'EOF'
# Development dependencies
-r requirements.txt

# Code quality
black>=23.0.0
flake8>=6.0.0
mypy>=1.0.0
pylint>=2.17.0

# Testing
pytest-asyncio>=0.21.0
pytest-xdist>=3.3.0  # Parallel test execution

# Documentation
sphinx>=7.0.0
EOF

echo "  ✅ .env.example"
echo "  ✅ requirements.txt"
echo "  ✅ requirements-dev.txt"

# =================================================================
# 8. CREATE README FILES
# =================================================================

echo ""
echo "📝 Creating documentation..."

# README for preprocessing
cat > preprocessing/README.md << 'EOF'
# Preprocessing Module

Text cleaning, chunking, and metadata enrichment for research papers.

## Modules

- `text_cleaner.py` - Clean and normalize paper text
- `chunker.py` - Split documents into semantic chunks
- `metadata_enricher.py` - Extract keywords and calculate importance scores

## Usage
```python
from preprocessing import TextCleaner, DocumentChunker

cleaner = TextCleaner()
chunker = DocumentChunker(chunk_size=512, overlap=50)

cleaned_text = cleaner.clean(raw_text)
chunks = chunker.chunk_document(cleaned_text, paper_id="123")
```
EOF

# README for embeddings
cat > embeddings/README.md << 'EOF'
# Embeddings Module

Embedding generation and vector storage using Weaviate.

## Modules

- `config.py` - Configuration management
- `embed_generator.py` - Generate embeddings
- `vector_store.py` - Weaviate integration
- `openai_embedder.py` - OpenAI fallback

## Usage
```python
from embeddings import EmbeddingGenerator

embedder = EmbeddingGenerator()
results = embedder.embed_chunks(chunks)
```
EOF

# README for services
cat > services/README.md << 'EOF'
# Services Module

Main microservices for CiteConnect.

## Services

- `embedding_service.py` - Process papers from GCS and generate embeddings

## Usage
```bash
python services/embedding_service.py healthcare --max-papers 100
```
EOF

echo "  ✅ Module documentation created"

# =================================================================
# 9. CREATE HELPER SCRIPTS
# =================================================================

echo ""
echo "📝 Creating helper scripts..."

# run_tests.sh
cat > run_tests.sh << 'EOF'
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
EOF

chmod +x run_tests.sh

# start_weaviate.sh
cat > start_weaviate.sh << 'EOF'
#!/bin/bash
# Start Weaviate with text2vec-transformers module

echo "Starting Weaviate with text2vec-transformers..."

docker run -d \
  --name weaviate \
  -p 8080:8080 \
  -p 50051:50051 \
  -e QUERY_DEFAULTS_LIMIT=25 \
  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  -e PERSISTENCE_DATA_PATH='/var/lib/weaviate' \
  -e DEFAULT_VECTORIZER_MODULE='text2vec-transformers' \
  -e ENABLE_MODULES='text2vec-transformers' \
  -e TRANSFORMERS_INFERENCE_API='http://t2v-transformers:8080' \
  -e CLUSTER_HOSTNAME='node1' \
  semitechnologies/weaviate:latest

echo "✅ Weaviate started on http://localhost:8080"
echo "Check status: curl http://localhost:8080/v1/meta"
EOF

chmod +x start_weaviate.sh

# stop_weaviate.sh
cat > stop_weaviate.sh << 'EOF'
#!/bin/bash
# Stop Weaviate container

echo "Stopping Weaviate..."
docker stop weaviate
docker rm weaviate
echo "✅ Weaviate stopped"
EOF

chmod +x stop_weaviate.sh

echo "  ✅ run_tests.sh"
echo "  ✅ start_weaviate.sh"
echo "  ✅ stop_weaviate.sh"

# =================================================================
# 10. UPDATE .gitignore
# =================================================================

echo ""
echo "📝 Updating .gitignore..."

cat >> .gitignore << 'EOF'

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
citeConnectEnv/
venv/
ENV/
env/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Environment variables
.env
.env.local

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Data
data/healthcare/*.parquet
data/finance/*.parquet
data/quantum/*.parquet
*.csv
*.parquet

# Logs
*.log
logs/

# Temporary files
*.tmp
temp/
tmp/

# Cloud credentials
*.json
!requirements*.json
service-account*.json
gcs-credentials.json

# macOS
.DS_Store

# Jupyter
.ipynb_checkpoints/
*.ipynb
EOF

echo "  ✅ .gitignore updated"

# =================================================================
# 11. CREATE PROJECT README
# =================================================================

echo ""
echo "📝 Creating main README..."

cat > README_EMBEDDING_SERVICE.md << 'EOF'
# CiteConnect Embedding Service

Production-ready embedding service for processing research papers from GCS.

## 🏗️ Architecture
```
GCS Parquet Files → Text Cleaning → Chunking → Embedding → Weaviate
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
# Create virtual environment
python3 -m venv citeConnectEnv
source citeConnectEnv/bin/activate  # On Windows: citeConnectEnv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Weaviate
```bash
./start_weaviate.sh
```

### 3. Configure Environment
```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### 4. Run Embedding Service
```bash
# Process healthcare domain
python services/embedding_service.py healthcare

# Process with limits (for testing)
python services/embedding_service.py healthcare --max-papers 100 --batch-size 25
```

## 📁 Project Structure
```
CITECONNECT/
├── preprocessing/          # Text cleaning and chunking
├── embeddings/            # Embedding generation and vector store
├── services/              # Main embedding service
├── utils/                 # Helper utilities (GCS, storage)
├── tests/                 # Test suite
├── data/                  # Local data storage
├── ingestion.py          # Data ingestion script
└── .env                  # Configuration (not in git)
```

## 🧪 Testing
```bash
# Run all tests
./run_tests.sh

# Run specific test
pytest tests/test_preprocessing.py -v
```

## 📊 Data Flow

1. **Input**: Parquet files in `gs://citeconnect-processed-parquet/{domain}/`
2. **Process**: Clean → Chunk → Embed
3. **Output**: Searchable vectors in Weaviate

## 🔧 Configuration

Edit `.env` to configure:
- GCS bucket and credentials
- Weaviate connection
- Embedding provider (Weaviate/OpenAI)
- Chunk size and overlap
- Batch processing size

## 📖 Module Documentation

- [Preprocessing Module](preprocessing/README.md)
- [Embeddings Module](embeddings/README.md)
- [Services Module](services/README.md)

## 🐛 Troubleshooting

**Weaviate not starting:**
```bash
docker logs weaviate
./stop_weaviate.sh
./start_weaviate.sh
```

**GCS authentication error:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/key.json"
```

**Import errors:**
```bash
pip install -r requirements.txt --upgrade
```
EOF

echo "  ✅ README_EMBEDDING_SERVICE.md"

# =================================================================
# 12. CREATE QUICK START GUIDE
# =================================================================

cat > QUICKSTART.md << 'EOF'
# Quick Start Guide

## Step 1: Setup
```bash
# Run setup script
bash setup.sh

# Activate virtual environment
source citeConnectEnv/bin/activate
```

## Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

## Step 3: Copy Code Files

Copy the code I provided into these files:

### Preprocessing Module
1. `preprocessing/text_cleaner.py`
2. `preprocessing/chunker.py`
3. `preprocessing/metadata_enricher.py`

### Embeddings Module
1. `embeddings/config.py`
2. `embeddings/embed_generator.py`
3. `embeddings/vector_store.py`

### Services
1. `services/embedding_service.py`

### Utils
1. `utils/gcs_reader.py`

## Step 4: Configure
```bash
# Copy environment template
cp .env.example .env

# Edit with your settings
nano .env
```

## Step 5: Start Weaviate
```bash
./start_weaviate.sh
```

## Step 6: Run Service
```bash
# Test with small dataset
python services/embedding_service.py healthcare --max-papers 10

# Process full domain
python services/embedding_service.py healthcare
```

## Step 7: Verify
```python
# Test in Python
from embeddings import WeaviateStore

store = WeaviateStore()
results = store.search("machine learning in healthcare", limit=5)
print(results)
```

Done! 🎉
EOF

echo "  ✅ QUICKSTART.md"

# =================================================================
# SUMMARY
# =================================================================

echo ""
echo "=================================================="
echo "   ✅ Setup Complete!"
echo "=================================================="
echo ""
echo "📂 Directory structure created:"
echo "   └── preprocessing/     (3 modules)"
echo "   └── embeddings/        (4 modules)"
echo "   └── services/          (1 service)"
echo "   └── utils/             (2 helpers)"
echo "   └── tests/             (test suite)"
echo ""
echo "📄 Configuration files created:"
echo "   └── .env.example"
echo "   └── requirements.txt"
echo "   └── requirements-dev.txt"
echo ""
echo "🔧 Helper scripts created:"
echo "   └── run_tests.sh"
echo "   └── start_weaviate.sh"
echo "   └── stop_weaviate.sh"
echo ""
echo "📖 Documentation created:"
echo "   └── README_EMBEDDING_SERVICE.md"
echo "   └── QUICKSTART.md"
echo ""
echo "${YELLOW}⚠️  NEXT STEPS:${NC}"
echo ""
echo "1. Copy code into the placeholder files"
echo "2. Configure .env with your credentials:"
echo "   ${GREEN}cp .env.example .env${NC}"
echo ""
echo "3. Install dependencies:"
echo "   ${GREEN}pip install -r requirements.txt${NC}"
echo ""
echo "4. Start Weaviate:"
echo "   ${GREEN}./start_weaviate.sh${NC}"
echo ""
echo "5. Run the service:"
echo "   ${GREEN}python services/embedding_service.py healthcare --max-papers 10${NC}"
echo ""
echo "📖 See QUICKSTART.md for detailed instructions"
echo ""
echo "=================================================="
EOF

chmod +x setup.sh
