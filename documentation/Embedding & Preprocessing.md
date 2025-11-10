# CiteConnect: Preprocessing and Embedding Documentation

**Version**: 1.0.0  
**Last Updated**: November 2024  
**Module**: Data Preprocessing & Embedding Generation Pipeline

---

## Table of Contents

1. [Overview](#1-overview)
2. [System Architecture](#2-system-architecture)
3. [Preprocessing Pipeline](#3-preprocessing-pipeline)
4. [Embedding Generation](#4-embedding-generation)
5. [Supported Providers](#5-supported-providers)
6. [Configuration](#6-configuration)
7. [Quick Start Guide](#7-quick-start-guide)
8. [API Reference](#8-api-reference)
9. [Usage Examples](#9-usage-examples)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Overview

### 1.1 Purpose

The preprocessing and embedding pipeline transforms raw academic paper text into searchable vector embeddings for semantic similarity search and intelligent recommendations.

### 1.2 Pipeline Stages

1. **Text Cleaning**: Normalize and clean raw paper text
2. **Chunking**: Split text into semantic segments with overlap
3. **Metadata Enrichment**: Add paper-level metadata to chunks
4. **Embedding Generation**: Create vector representations
5. **Storage & Indexing**: Save embeddings for fast retrieval

### 1.3 Key Features

- **Multi-Provider Support**: Local (Sentence-Transformers), Weaviate, OpenAI
- **Intelligent Chunking**: 512 tokens with 50-token overlap
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Automatic Persistence**: Auto-save embeddings after generation
- **Production Ready**: 85%+ test coverage, comprehensive error handling

---

## 2. System Architecture

### 2.1 Data Flow
```
┌─────────────┐
│  GCS Papers │ (Parquet format)
└──────┬──────┘
       │
       ↓
┌─────────────────────┐
│  Text Preprocessing │
│  • Clean text       │
│  • Remove artifacts │
│  • Normalize        │
└──────┬──────────────┘
       │
       ↓
┌─────────────────────┐
│   Text Chunking     │
│  • Split 512 tokens │
│  • 50 token overlap │
│  • Preserve sentences│
└──────┬──────────────┘
       │
       ↓
┌─────────────────────┐
│ Metadata Enrichment │
│  • Keywords         │
│  • Importance score │
│  • Domain classify  │
└──────┬──────────────┘
       │
       ↓
┌─────────────────────┐
│ Embedding Generation│
│  • Local/Weaviate   │
│  • OpenAI           │
│  • Batch process    │
└──────┬──────────────┘
       │
       ↓
┌─────────────────────┐
│  Vector Storage     │
│  • Local pickle     │
│  • Weaviate DB      │
│  • Search index     │
└─────────────────────┘
```

### 2.2 Component Structure
```
src/DataPipeline/
├── preprocessing/
│   ├── text_cleaner.py         # Text normalization
│   ├── chunker.py              # Text segmentation (DocumentChunker class)
│   └── metadata_enricher.py    # Metadata enrichment (MetadataEnricher class)
│
└── embeddings/
    ├── config.py               # Configuration management
    ├── embed_generator.py      # Provider abstraction (EmbeddingGenerator class)
    ├── local_embedder.py       # Local implementation (LocalEmbedder class)
    ├── openai_embedder.py      # OpenAI implementation (empty - handled in embed_generator)
    └── vector_store.py         # Weaviate integration (WeaviateStore class)

services/
└── embedding_service.py        # Main orchestration (EmbeddingService class)

utils/
└── gcs_reader.py              # GCS/Local file reading
```

---

## 3. Preprocessing Pipeline

### 3.1 Text Cleaner

**Purpose**: Normalize and clean academic paper text for optimal embedding quality.

**Class**: `TextCleaner`  
**Location**: `src/DataPipeline/preprocessing/text_cleaner.py`

**Operations**:
- Remove reference sections
- Remove headers and footers
- Remove emails and URLs
- Remove citations (optional)
- Normalize whitespace
- Remove PDF extraction artifacts

**Usage**:
```python
from src.DataPipeline.preprocessing.text_cleaner import TextCleaner

cleaner = TextCleaner(remove_citations=True, remove_urls=True)
clean_text = cleaner.clean(raw_text)

# Batch cleaning
cleaned_texts = cleaner.clean_batch([text1, text2, text3])
```

**Parameters**:
- `remove_citations` (bool): Remove in-text citations like [1,2,3] (default: False)
- `remove_urls` (bool): Remove URLs (default: True)

### 3.2 Document Chunker

**Purpose**: Split long documents into semantic chunks with overlap for context preservation.

**Class**: `DocumentChunker`  
**Location**: `src/DataPipeline/preprocessing/chunker.py`

**Parameters**:
- `chunk_size`: 512 tokens (default)
- `overlap`: 50 tokens (default)
- `min_chunk_size`: 100 tokens (default)

**Usage**:
```python
from src.DataPipeline.preprocessing.chunker import DocumentChunker

chunker = DocumentChunker(chunk_size=512, overlap=50, min_chunk_size=100)
chunks = chunker.chunk_document(
    text=clean_text,
    paper_id="paper1",
    preserve_sentences=True
)
```

**Output Format** (Chunk dataclass):
```python
@dataclass
class Chunk:
    chunk_id: str           # "paper1_chunk_0"
    paper_id: str           # "paper1"
    text: str               # Chunk content
    position: int           # Position in paper
    token_count: int        # Estimated tokens
    start_char: int         # Start character position
    end_char: int           # End character position
```

**Note**: The chunker returns `Chunk` objects, not dictionaries. Convert to dict if needed:
```python
chunk_dict = {
    'chunk_id': chunk.chunk_id,
    'paper_id': chunk.paper_id,
    'text': chunk.text,
    'position': chunk.position,
    'token_count': chunk.token_count
}
```

### 3.3 Metadata Enricher

**Purpose**: Add derived metadata fields to paper metadata.

**Class**: `MetadataEnricher`  
**Location**: `src/DataPipeline/preprocessing/metadata_enricher.py`

**Enriched Fields**:
- `keywords`: Top 10 extracted keywords
- `importance_score`: 0-100 score based on citations and recency
- `research_domain`: Classified domain (Computer Science, Medicine, etc.)
- `decade`: Publication decade

**Usage**:
```python
from src.DataPipeline.preprocessing.metadata_enricher import MetadataEnricher

enricher = MetadataEnricher(top_k_keywords=10)

# Enrich single paper metadata
enriched_metadata = enricher.enrich(paper_metadata)

# Extract keywords
keywords = enricher.extract_keywords("text to analyze")

# Calculate importance
score = enricher.calculate_importance_score(paper_metadata)

# Classify domain
domain = enricher.classify_domain(paper_metadata)
```

**Input Format**:
```python
paper_metadata = {
    'paperId': 'paper1',
    'title': 'Deep Learning Survey',
    'abstract': 'This paper surveys...',
    'year': 2023,
    'citationCount': 45,
    'influentialCitationCount': 12,
    'fieldsOfStudy': ['Computer Science', 'Machine Learning']
}
```

---

## 4. Embedding Generation

### 4.1 Embedding Generator

**Purpose**: Provider-agnostic embedding generation with automatic batching.

**Class**: `EmbeddingGenerator`  
**Location**: `src/DataPipeline/embeddings/embed_generator.py`

**Usage**:
```python
from src.DataPipeline.embeddings.embed_generator import EmbeddingGenerator

generator = EmbeddingGenerator(provider='local')
results = generator.embed_chunks(chunks, show_progress=True)
```

**Methods**:

| Method | Description | Returns |
|--------|-------------|---------|
| `embed_chunks(chunks, show_progress)` | Generate embeddings for chunk list | `List[EmbeddingResult]` |
| `embed_single(text)` | Generate embedding for single text | `List[float]` or `None` |
| `search(query, limit)` | Find similar chunks | `List[Dict]` |

**EmbeddingResult Format**:
```python
@dataclass
class EmbeddingResult:
    chunk_id: str
    embedding: List[float]  # Empty for Weaviate
    success: bool
    error: Optional[str] = None
```

### 4.2 Local Embedder

**Purpose**: Sentence-transformers implementation for local embedding generation.

**Class**: `LocalEmbedder`  
**Location**: `src/DataPipeline/embeddings/local_embedder.py`

**Usage**:
```python
from src.DataPipeline.embeddings.local_embedder import LocalEmbedder

# Initialize
embedder = LocalEmbedder(
    model_name='all-MiniLM-L6-v2',
    save_path='working_data/embeddings_db.pkl'
)

# Generate embeddings
results = embedder.embed_chunks(chunks, show_progress=True)

# Search
matches = embedder.search("machine learning", limit=5)

# Get statistics
stats = embedder.get_stats()
# {'total_chunks': 150, 'embedding_dimension': 384, 'unique_papers': 10, 'save_path': '...'}

# Save/Load
embedder.save('path/to/embeddings.pkl')
embedder.load('path/to/embeddings.pkl')
```

**Search Result Format**:
```python
{
    "chunk_id": "paper1_chunk_0",
    "paper_id": "paper1",
    "text": "Machine learning is...",
    "paper_title": "Deep Learning Survey",
    "paper_year": 2023,
    "citation_count": 45,
    "similarity": 0.867,      # 0.0 to 1.0
    "distance": 0.133,        # 1 - similarity
    "position": 0
}
```

### 4.3 Weaviate Vector Store

**Purpose**: Production-grade vector database integration with automatic vectorization.

**Class**: `WeaviateStore`  
**Location**: `src/DataPipeline/embeddings/vector_store.py`

**Usage**:
```python
from src.DataPipeline.embeddings.vector_store import WeaviateStore

# Initialize (auto-connects and creates collection)
store = WeaviateStore()

# Add chunks
success = store.add_chunks(chunks)

# Search
results = store.search("deep learning", limit=10)

# Close connection
store.close()
```

**Schema**:
```python
Collection: PaperChunks
Properties:
  - chunk_id: TEXT
  - paper_id: TEXT
  - text: TEXT (auto-vectorized)
  - position: INT
  - token_count: INT
  - paper_title: TEXT
  - paper_year: INT
  - citation_count: INT
```

---

## 5. Supported Providers

### 5.1 Provider Comparison

| Feature | Local | Weaviate | OpenAI |
|---------|-------|----------|--------|
| **Setup** | Easy | Medium | Easy |
| **Cost** | Free | Free/Paid | $0.02/1M tokens |
| **Scale** | <100K papers | 100K+ papers | Unlimited |
| **Privacy** | Complete | Complete | Limited |
| **Speed** | Fast | Very Fast | Medium |
| **Dimensions** | 384-768 | 768 | 1536 |
| **Offline** | Yes | Yes | No |

### 5.2 Selection Guide

**Choose Local if**:
- Development or testing
- Dataset < 100K papers
- Privacy is critical
- No budget for infrastructure

**Choose Weaviate if**:
- Production deployment
- Dataset > 100K papers
- Need advanced filtering
- High availability required

**Choose OpenAI if**:
- Need highest quality
- Low volume usage
- Limited local compute

---

## 6. Configuration

### 6.1 Environment Variables
```bash
# Provider Selection
EMBEDDING_PROVIDER=local              # local, weaviate, openai

# Local Provider Settings
SENTENCE_TRANSFORMERS_MODEL=all-MiniLM-L6-v2
LOCAL_EMBEDDINGS_PATH=working_data/embeddings_db.pkl

# Weaviate Settings
WEAVIATE_URL=http://localhost:8080
WEAVIATE_API_KEY=
WEAVIATE_COLLECTION=PaperChunks

# OpenAI Settings
OPENAI_API_KEY=sk-your-key-here
OPENAI_EMBEDDING_MODEL=text-embedding-3-small

# Processing Parameters
CHUNK_SIZE=512
CHUNK_OVERLAP=50
EMBEDDING_BATCH_SIZE=32

# GCS Settings
GOOGLE_APPLICATION_CREDENTIALS=config/gcp-credentials.json
GCS_BUCKET_NAME=citeconnect-test-bucket
GCS_PROJECT_ID=your-gcp-project-id
```

### 6.2 Configuration Class

**Location**: `src/DataPipeline/embeddings/config.py`
```python
from src.DataPipeline.embeddings.config import config

# Access configuration
print(f"Provider: {config.provider}")
print(f"Model: {config.local_model}")
print(f"Batch size: {config.batch_size}")

# Validate
config.validate()  # Raises ValueError if invalid
```

---

## 7. Quick Start Guide

### 7.1 Installation
```bash
# Clone and setup
git clone https://github.com/DhikshaMathanagopal/CiteConnect.git
cd CiteConnect

# Create virtual environment
python3.9 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### 7.2 Configuration
```bash
# Copy and edit environment file
cp .env.example .env
nano .env
```

### 7.3 Generate Embeddings Using Service

**Option 1: Process from GCS**
```bash
python services/embedding_service.py healthcare \
    --batch-size 50 \
    --max-papers 100 \
    --bucket citeconnect-test-bucket \
    --gcs-prefix raw/ \
    --gcs-project your-project-id
```

**Option 2: Process from Local Files**
```bash
python services/embedding_service.py healthcare \
    --local \
    --batch-size 50 \
    --max-papers 10
```

**Option 3: Streaming Mode (Memory Efficient)**
```bash
python services/embedding_service.py finance \
    --streaming \
    --batch-size 50
```

**Option 4: Flat Structure (No Domain Subfolders)**
```bash
python services/embedding_service.py healthcare \
    --flat-structure \
    --gcs-prefix raw/ \
    --max-papers 50
```

### 7.4 Search
```python
from src.DataPipeline.embeddings.local_embedder import LocalEmbedder

# Load embeddings
embedder = LocalEmbedder(save_path='working_data/embeddings_db.pkl')

# Search
results = embedder.search("machine learning healthcare", limit=5)

# Display
for result in results:
    print(f"{result['paper_title']}: {result['similarity']:.3f}")
```

---

## 8. API Reference

### 8.1 Preprocessing Classes

#### TextCleaner
```python
class TextCleaner:
    def __init__(remove_citations: bool = False, remove_urls: bool = True)
    def clean(text: str) -> str
    def clean_batch(texts: List[str]) -> List[str]
```

#### DocumentChunker
```python
class DocumentChunker:
    def __init__(chunk_size: int = 512, overlap: int = 50, min_chunk_size: int = 100)
    def chunk_document(text: str, paper_id: str, preserve_sentences: bool = True) -> List[Chunk]
```

#### MetadataEnricher
```python
class MetadataEnricher:
    def __init__(top_k_keywords: int = 10)
    def enrich(paper_metadata: Dict) -> Dict
    def extract_keywords(text: str, use_tfidf: bool = False) -> List[str]
    def calculate_importance_score(metadata: Dict) -> float
    def classify_domain(metadata: Dict) -> str
```

### 8.2 Embedding Classes

#### EmbeddingGenerator
```python
class EmbeddingGenerator:
    def __init__(provider: Optional[str] = None)
    def embed_chunks(chunks: List[Dict], show_progress: bool = True) -> List[EmbeddingResult]
    def embed_single(text: str) -> Optional[List[float]]
    def search(query: str, limit: int = 10) -> List[Dict]
```

#### LocalEmbedder
```python
class LocalEmbedder:
    def __init__(model_name: Optional[str] = None, save_path: Optional[str] = None)
    def embed_chunks(chunks: List[Dict], show_progress: bool = False) -> List[Dict]
    def search(query: str, limit: int = 10) -> List[Dict]
    def save(filepath: Optional[str] = None)
    def load(filepath: Optional[str] = None)
    def get_stats() -> Dict
```

#### WeaviateStore
```python
class WeaviateStore:
    def __init__()
    def add_chunks(chunks: List[Dict]) -> bool
    def search(query: str, limit: int = 10, filters: Optional[Dict] = None) -> List[Dict]
    def delete_collection()
    def close()
```

#### EmbeddingService
```python
class EmbeddingService:
    def __init__(bucket_name: str, use_local: bool, gcs_prefix: str, flat_structure: bool, gcs_project_id: Optional[str])
    def process_domain(domain: str, batch_size: int, max_papers: Optional[int], use_streaming: bool) -> Dict[str, int]
```

### 8.3 Data Structures

**Chunk Object** (from DocumentChunker):
```python
@dataclass
class Chunk:
    chunk_id: str
    paper_id: str
    text: str
    position: int
    token_count: int
    start_char: int
    end_char: int
```

**Chunk Dict** (for embeddings):
```python
{
    "chunk_id": str,
    "paper_id": str,
    "text": str,
    "position": int,
    "token_count": int,
    "paper_title": str,
    "paper_year": int,
    "citation_count": int
}
```

**EmbeddingResult**:
```python
@dataclass
class EmbeddingResult:
    chunk_id: str
    embedding: List[float]
    success: bool
    error: Optional[str] = None
```

---

## 9. Usage Examples

### 9.1 Complete Preprocessing Pipeline
```python
from src.DataPipeline.preprocessing.text_cleaner import TextCleaner
from src.DataPipeline.preprocessing.chunker import DocumentChunker
from src.DataPipeline.preprocessing.metadata_enricher import MetadataEnricher

# Initialize
cleaner = TextCleaner(remove_citations=True)
chunker = DocumentChunker(chunk_size=512, overlap=50)
enricher = MetadataEnricher(top_k_keywords=10)

# Paper data
paper = {
    'paperId': 'paper1',
    'title': 'Machine Learning in Healthcare',
    'abstract': 'This paper surveys...',
    'introduction': 'Machine learning has revolutionized...',
    'year': 2023,
    'citationCount': 45,
    'authors': ['Smith, J.']
}

# Step 1: Clean
raw_text = paper['abstract'] + "\n\n" + paper['introduction']
clean_text = cleaner.clean(raw_text)

# Step 2: Chunk (returns Chunk objects)
chunks = chunker.chunk_document(clean_text, paper_id=paper['paperId'])

# Step 3: Enrich paper metadata (optional)
enriched_metadata = enricher.enrich(paper)

# Step 4: Convert chunks to dicts for embedding
chunk_dicts = []
for chunk in chunks:
    chunk_dicts.append({
        'chunk_id': chunk.chunk_id,
        'paper_id': chunk.paper_id,
        'text': chunk.text,
        'position': chunk.position,
        'token_count': chunk.token_count,
        'paper_title': paper['title'],
        'paper_year': paper['year'],
        'citation_count': paper['citationCount']
    })

print(f"Created {len(chunk_dicts)} enriched chunks")
```

### 9.2 Local Embedding Generation
```python
from src.DataPipeline.embeddings.local_embedder import LocalEmbedder

# Initialize
embedder = LocalEmbedder()

# Prepare chunks
chunks = [{
    "chunk_id": "p1_c0",
    "paper_id": "paper1",
    "text": "Machine learning enables automated learning from data.",
    "paper_title": "ML Survey",
    "paper_year": 2023,
    "citation_count": 10
}]

# Generate embeddings
results = embedder.embed_chunks(chunks, show_progress=True)

# Check success
for result in results:
    print(f"Chunk {result['chunk_id']}: {result['success']}")

# Get statistics
stats = embedder.get_stats()
print(f"Total chunks: {stats['total_chunks']}")
```

### 9.3 Search and Retrieval
```python
from src.DataPipeline.embeddings.local_embedder import LocalEmbedder

# Load existing embeddings
embedder = LocalEmbedder(save_path='working_data/embeddings_db.pkl')

# Search
query = "deep learning for medical image analysis"
results = embedder.search(query, limit=5)

# Display
for i, result in enumerate(results, 1):
    print(f"\n{i}. {result['paper_title']} ({result['paper_year']})")
    print(f"   Similarity: {result['similarity']:.3f}")
    print(f"   Preview: {result['text'][:100]}...")
```

### 9.4 Using Embedding Service

**Complete workflow with EmbeddingService**:
```python
from services.embedding_service import EmbeddingService

# Initialize service
service = EmbeddingService(
    bucket_name="citeconnect-test-bucket",
    use_local=False,
    gcs_prefix="raw/",
    flat_structure=False,
    gcs_project_id="your-project-id"
)

# Process domain
stats = service.process_domain(
    domain="healthcare",
    batch_size=50,
    max_papers=100,
    use_streaming=False
)

# Print results
print(f"Processed: {stats['processed_papers']} papers")
print(f"Embedded: {stats['embedded_chunks']} chunks")
```

### 9.5 Weaviate Integration
```bash
# Start Weaviate with Docker
docker run -d -p 8080:8080 \
  -e ENABLE_MODULES='text2vec-transformers' \
  -e DEFAULT_VECTORIZER_MODULE='text2vec-transformers' \
  semitechnologies/weaviate:latest
```
```python
from src.DataPipeline.embeddings.vector_store import WeaviateStore

# Initialize
store = WeaviateStore()

# Add chunks
chunks = [{
    "chunk_id": "p1_c0",
    "paper_id": "paper1",
    "text": "Machine learning enables automated decision making.",
    "position": 0,
    "token_count": 150,
    "paper_title": "ML Survey",
    "paper_year": 2023,
    "citation_count": 45
}]

success = store.add_chunks(chunks)
print(f"Added chunks: {success}")

# Search
results = store.search("machine learning", limit=5)

for result in results:
    print(f"{result['paper_title']}: {result['distance']:.3f}")
```

---

## 10. Troubleshooting

### 10.1 Common Issues

#### Issue: Model Download Fails
```bash
# Pre-download model
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
```

#### Issue: Weaviate Connection Failed
```bash
# Check if running
curl http://localhost:8080/v1/meta

# Start Weaviate
docker run -d -p 8080:8080 \
  -e ENABLE_MODULES='text2vec-transformers' \
  semitechnologies/weaviate:latest
```

#### Issue: Out of Memory
```bash
# Reduce batch size
EMBEDDING_BATCH_SIZE=16
CHUNK_SIZE=256
```

#### Issue: GCS Access Denied
```bash
# Check credentials
ls -l config/gcp-credentials.json

# Verify bucket access
gsutil ls gs://citeconnect-test-bucket

# Set project
gcloud config set project your-project-id
```

#### Issue: Embeddings Not Persisting
```python
# Explicitly save
embedder.save('working_data/embeddings_db.pkl')

# Verify save location
print(f"Saved to: {embedder.save_path}")
```

#### Issue: Empty Search Results
```python
# Check if embeddings exist
stats = embedder.get_stats()
print(f"Total chunks: {stats['total_chunks']}")

# If 0, generate embeddings first
if stats['total_chunks'] == 0:
    print("No embeddings found - run embedding generation first")
```

### 10.2 Validation Commands
```bash
# Test imports
python -c "from src.DataPipeline.embeddings.local_embedder import LocalEmbedder; print('✅ OK')"

# Test configuration
python -c "from src.DataPipeline.embeddings.config import config; config.validate(); print('✅ OK')"

# Test preprocessing
python -c "from src.DataPipeline.preprocessing.chunker import DocumentChunker; print('✅ OK')"

# Run tests
pytest tests/unit/test_embeddings.py -v

# Test service
python services/embedding_service.py healthcare --local --max-papers 5
```

### 10.3 Debugging Tips
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check GPU
import torch
print(f"CUDA: {torch.cuda.is_available()}")

# Verify model
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
print(f"Dimension: {model.get_sentence_embedding_dimension()}")

# Test chunking
from src.DataPipeline.preprocessing.chunker import DocumentChunker
chunker = DocumentChunker(chunk_size=512, overlap=50)
chunks = chunker.chunk_document("Test text" * 100, "test")
print(f"Generated {len(chunks)} chunks")
```

---

## Additional Resources

- **Sentence-Transformers**: https://www.sbert.net/
- **Weaviate Documentation**: https://weaviate.io/developers/weaviate
- **OpenAI Embeddings**: https://platform.openai.com/docs/guides/embeddings
- **Project Repository**: https://github.com/DhikshaMathanagopal/CiteConnect

---

**Version**: 1.0.0  
**Status**: ✅ Production Ready  
**License**: MIT  
**Maintainers**: CiteConnect Team, Northeastern University
