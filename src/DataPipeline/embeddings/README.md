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
