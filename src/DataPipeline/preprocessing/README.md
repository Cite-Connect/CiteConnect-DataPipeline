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
