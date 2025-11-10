"""
CiteConnect Embedding Service
Processes parquet files from GCS or local storage and generates embeddings.
Supports both Weaviate and local sentence-transformers.
"""

import logging
import pandas as pd
from typing import Dict, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.DataPipeline.preprocessing import TextCleaner, DocumentChunker
from src.DataPipeline.embeddings import EmbeddingGenerator

# Enhanced logging with file and line numbers
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EmbeddingService:
    """Main service for processing papers and generating embeddings."""

    def __init__(self, bucket_name: str = "citeconnect-processed-parquet", use_local: bool = False, gcs_prefix: str = "", flat_structure: bool = False, gcs_project_id: Optional[str] = None):
        """
        Initialize embedding service.

        Args:
            bucket_name: GCS bucket containing parquet files (ignored if use_local=True)
            use_local: Use local parquet files instead of GCS (for testing)
            gcs_prefix: GCS path prefix (e.g., 'raw/' for citeconnect-test-bucket)
            flat_structure: Files are in flat structure (no domain subfolders)
            gcs_project_id: GCP project ID for cross-project bucket access
        """
        logger.info("Initializing Embedding Service...")
        
        # Initialize appropriate reader
        if use_local:
            from utils.gcs_reader import LocalFileReader
            self.gcs_reader = LocalFileReader(base_dir="data")
            logger.info("📁 Using LOCAL file reader (no GCS credentials needed)")
        else:
            from utils.gcs_reader import GCSReader
            self.gcs_reader = GCSReader(bucket_name=bucket_name, project_id=gcs_project_id)
            self.gcs_prefix = gcs_prefix  # Store for later use
            self.flat_structure = flat_structure
            logger.info(f"🌐 Using GCS reader for bucket: {bucket_name}")
            if gcs_project_id:
                logger.info(f"🏗️  Using project: {gcs_project_id}")
            if gcs_prefix:
                logger.info(f"📂 GCS prefix: {gcs_prefix}")
            if flat_structure:
                logger.info(f"📁 Using flat structure (all files in {gcs_prefix})")
        
        # Initialize processing components
        self.text_cleaner = TextCleaner(remove_citations=True)
        self.chunker = DocumentChunker(chunk_size=512, overlap=50)
        self.embedder = EmbeddingGenerator()
        
        logger.info("✅ Embedding Service initialized successfully")

    def process_domain(
        self,
        domain: str,
        batch_size: int = 50,
        max_papers: Optional[int] = None,
        use_streaming: bool = False
    ) -> Dict[str, int]:
        """
        Process all papers from a domain.

        Args:
            domain (str): Domain folder (e.g., 'healthcare', 'finance')
            batch_size (int): Papers to process per batch (default: 50)
            max_papers (Optional[int]): Maximum papers to process, None = all (default: None)
            use_streaming (bool): Use memory-efficient streaming for large datasets (default: False)

        Returns:
            Dict[str, int]: Statistics dict with processing counts
        """
        logger.info(f"🚀 Starting processing for domain: {domain}")
        
        if use_streaming:
            return self._process_domain_streaming(domain, batch_size, max_papers)
        else:
            return self._process_domain_standard(domain, batch_size, max_papers)

    def _process_domain_standard(
        self,
        domain: str,
        batch_size: int,
        max_papers: Optional[int]
    ) -> Dict[str, int]:
        """Standard processing - loads all data into memory."""
        # Load all parquet files from domain
        logger.info(f"📂 Loading parquet files from domain: {domain}")
        
        # Use custom prefix if GCS is being used
        if hasattr(self, 'gcs_prefix') and self.gcs_prefix:
            flat = getattr(self, 'flat_structure', False)
            if flat:
                # Flat structure: read all files from prefix, ignore domain
                df = self.gcs_reader.read_all_from_domain(
                    domain=domain, 
                    custom_prefix=self.gcs_prefix,
                    flat_structure=True
                )
            else:
                # Hierarchical: prefix + domain
                full_prefix = f"{self.gcs_prefix}{domain}/"
                df = self.gcs_reader.read_all_from_domain(domain, custom_prefix=full_prefix)
        else:
            df = self.gcs_reader.read_all_from_domain(domain)
        
        if df.empty:
            logger.error(f"❌ No data found for domain: {domain}")
            return {
                "error": "No data found",
                "total_papers": 0,
                "processed_papers": 0,
                "skipped_papers": 0,
                "total_chunks": 0,
                "embedded_chunks": 0
            }

        if max_papers:
            df = df.head(max_papers)
            logger.info(f"📊 Limited to {max_papers} papers for processing")

        logger.info(f"📚 Loaded {len(df)} papers from domain: {domain}")
        
        # Process in batches
        stats = {
            "total_papers": len(df),
            "processed_papers": 0,
            "skipped_papers": 0,
            "total_chunks": 0,
            "embedded_chunks": 0
        }

        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(df) - 1) // batch_size + 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_df)} papers)")
            logger.info(f"{'='*60}")
            
            batch_stats = self._process_batch(batch_df)
            
            # Update stats
            stats["processed_papers"] += batch_stats["processed"]
            stats["skipped_papers"] += batch_stats["skipped"]
            stats["total_chunks"] += batch_stats["chunks"]
            stats["embedded_chunks"] += batch_stats["embedded"]
            
            logger.info(
                f"Batch {batch_num} complete: "
                f"Processed={batch_stats['processed']}, "
                f"Skipped={batch_stats['skipped']}, "
                f"Chunks={batch_stats['chunks']}, "
                f"Embedded={batch_stats['embedded']}"
            )

        logger.info(f"\n✅ Domain processing complete")
        return stats

    def _process_domain_streaming(
        self,
        domain: str,
        batch_size: int,
        max_papers: Optional[int]
    ) -> Dict[str, int]:
        """
        Memory-efficient streaming processing.
        Uses generator to process batches without loading all data.
        """
        logger.info(f"🌊 Using streaming mode for domain: {domain}")
        
        stats = {
            "total_papers": 0,
            "processed_papers": 0,
            "skipped_papers": 0,
            "total_chunks": 0,
            "embedded_chunks": 0
        }

        batch_num = 0
        papers_processed = 0

        # Use generator to stream batches
        for batch_df in self.gcs_reader.stream_parquet_batches(domain, batch_size):
            batch_num += 1
            
            # Check max_papers limit
            if max_papers and papers_processed >= max_papers:
                logger.info(f"Reached max_papers limit ({max_papers}), stopping")
                break

            # Trim batch if needed to respect max_papers
            if max_papers and papers_processed + len(batch_df) > max_papers:
                remaining = max_papers - papers_processed
                batch_df = batch_df.head(remaining)
                logger.info(f"Trimming batch to {remaining} papers to respect max_papers")

            logger.info(f"\n{'='*60}")
            logger.info(f"📦 Processing streamed batch {batch_num}")
            logger.info(f"{'='*60}")
            
            batch_stats = self._process_batch(batch_df)
            
            # Update stats
            stats["total_papers"] += len(batch_df)
            stats["processed_papers"] += batch_stats["processed"]
            stats["skipped_papers"] += batch_stats["skipped"]
            stats["total_chunks"] += batch_stats["chunks"]
            stats["embedded_chunks"] += batch_stats["embedded"]
            
            papers_processed += len(batch_df)

        logger.info(f"\n✅ Streaming processing complete")
        return stats

    def _process_batch(self, batch_df: pd.DataFrame) -> Dict[str, int]:
        """Process a batch of papers."""
        batch_chunks = []
        stats = {"processed": 0, "skipped": 0, "chunks": 0, "embedded": 0}

        for idx, paper in batch_df.iterrows():
            paper_id = paper.get('paperId')
            
            # Try to get content (prefer introduction, fallback to abstract)
            content = None
            content_source = None
            
            # First try introduction (regardless of has_intro flag)
            if pd.notna(paper.get('introduction')) and paper.get('introduction'):
                content = str(paper['introduction'])
                content_source = 'introduction'
                logger.debug(f"Using introduction for paper {paper_id} (method: {paper.get('extraction_method', 'unknown')})")
            # Fallback to abstract if no introduction
            elif pd.notna(paper.get('abstract')) and paper.get('abstract'):
                content = str(paper['abstract'])
                content_source = 'abstract'
                logger.debug(f"Using abstract for paper {paper_id} (no introduction available)")
            
            if not content:
                logger.debug(f"⏭️  Skipping paper {paper_id} - no introduction or abstract")
                stats["skipped"] += 1
                continue

            try:
                # Clean text
                cleaned_text = self.text_cleaner.clean(content)
                
                if len(cleaned_text) < 200:
                    logger.debug(
                        f"⏭️  Skipping paper {paper_id} - text too short after cleaning "
                        f"({len(cleaned_text)} chars from {content_source})"
                    )
                    stats["skipped"] += 1
                    continue

                # Chunk document
                chunks = self.chunker.chunk_document(cleaned_text, paper_id)
                
                if len(cleaned_text) < 200:
                    logger.debug(f"⏭️  Skipping paper {paper_id} - text too short after cleaning ({len(cleaned_text)} chars)")
                    stats["skipped"] += 1
                    continue

                # Chunk document
                chunks = self.chunker.chunk_document(cleaned_text, paper_id)
                
                if not chunks:
                    logger.warning(f"⚠️  No chunks generated for paper {paper_id}")
                    stats["skipped"] += 1
                    continue

                # Prepare chunks for embedding with metadata
                for chunk in chunks:
                    chunk_dict = {
                        'chunk_id': chunk.chunk_id,
                        'paper_id': chunk.paper_id,
                        'text': chunk.text,
                        'position': chunk.position,
                        'token_count': chunk.token_count,
                        'paper_title': str(paper.get('title', '')) if pd.notna(paper.get('title')) else '',
                        'paper_year': int(paper.get('year', 0)) if pd.notna(paper.get('year')) else 0,
                        'citation_count': int(paper.get('citationCount', 0)) if pd.notna(paper.get('citationCount')) else 0,
                        'extraction_method': str(paper.get('extraction_method', '')) if pd.notna(paper.get('extraction_method')) else '',
                        'content_quality': str(paper.get('content_quality', '')) if pd.notna(paper.get('content_quality')) else '',
                    }
                    batch_chunks.append(chunk_dict)

                stats["processed"] += 1
                stats["chunks"] += len(chunks)
                
                logger.debug(f"✅ Processed paper {paper_id}: {len(chunks)} chunks created")

            except Exception as e:
                logger.error(f"❌ Failed to process paper {paper_id}: {e}", exc_info=True)
                stats["skipped"] += 1

        # Generate embeddings for batch
        if batch_chunks:
            logger.info(f"🧬 Generating embeddings for {len(batch_chunks)} chunks...")
            try:
                results = self.embedder.embed_chunks(batch_chunks, show_progress=False)
                stats["embedded"] = sum(1 for r in results if r.success)
                
                if stats["embedded"] < len(batch_chunks):
                    logger.warning(
                        f"⚠️ Only {stats['embedded']}/{len(batch_chunks)} chunks embedded successfully"
                    )
                else:
                    logger.info(f"✅ Successfully embedded all {stats['embedded']} chunks")
            except Exception as e:
                logger.error(f"❌ Embedding generation failed: {e}", exc_info=True)
                stats["embedded"] = 0
        else:
            logger.info("ℹ️  No chunks to embed in this batch")

        return stats


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="CiteConnect Embedding Service - Process papers from GCS or local files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process from local files (testing)
  python services/embedding_service.py healthcare --local --max-papers 10
  
  # Process from GCS (production)
  python services/embedding_service.py healthcare --max-papers 100
  
  # Process all papers with streaming (memory-efficient)
  python services/embedding_service.py finance --streaming
        """
    )
    parser.add_argument(
        "domain",
        help="Domain to process (e.g., healthcare, finance, quantum)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Papers per batch (default: 50)"
    )
    parser.add_argument(
        "--max-papers",
        type=int,
        default=None,
        help="Maximum papers to process (default: all)"
    )
    parser.add_argument(
        "--bucket",
        default="citeconnect-test-bucket",
        help="GCS bucket name (default: citeconnect-test-bucket)"
    )
    parser.add_argument(
        "--gcs-prefix",
        dest="gcs_prefix",
        default="raw/",
        help="GCS path prefix (default: raw/)"
    )
    parser.add_argument(
        "--gcs-project",
        dest="gcs_project",
        default="strange-calling-476017-r5",
        help="GCP project ID for bucket access (default: strange-calling-476017-r5)"
    )
    parser.add_argument(
        "--flat-structure",
        action="store_true",
        help="Files are in flat structure (no domain subfolders in GCS)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local parquet files from data/{domain}/ instead of GCS"
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Use memory-efficient streaming mode (for large datasets)"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize service
        logger.info("\n" + "="*60)
        logger.info("CITECONNECT EMBEDDING SERVICE")
        logger.info("="*60)
        
        service = EmbeddingService(
            bucket_name=args.bucket, 
            use_local=args.local,
            gcs_prefix=args.gcs_prefix,
            flat_structure=args.flat_structure,
            gcs_project_id=args.gcs_project if not args.local else None
        )
        
        # Process domain
        stats = service.process_domain(
            domain=args.domain,
            batch_size=args.batch_size,
            max_papers=args.max_papers,
            use_streaming=args.streaming
        )
        
        # Print summary
        print("\n" + "="*60)
        print("EMBEDDING SERVICE - PROCESSING COMPLETE")
        print("="*60)
        print(f"Domain:            {args.domain}")
        print(f"Mode:              {'LOCAL FILES' if args.local else 'GCS BUCKET'}")
        print(f"Total Papers:      {stats.get('total_papers', 0)}")
        print(f"Processed:         {stats.get('processed_papers', 0)}")
        print(f"Skipped:           {stats.get('skipped_papers', 0)}")
        print(f"Total Chunks:      {stats.get('total_chunks', 0)}")
        print(f"Embedded Chunks:   {stats.get('embedded_chunks', 0)}")
        
        if stats.get('embedded_chunks', 0) > 0:
            print("\n✅ SUCCESS: Embeddings generated and stored")
        else:
            print("\n⚠️  WARNING: No embeddings were generated")
        
        print("="*60 + "\n")
        
        return 0 if stats.get('embedded_chunks', 0) > 0 else 1
        
    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR: {e}", exc_info=True)
        print("\n" + "="*60)
        print("EMBEDDING SERVICE - FAILED")
        print("="*60)
        print(f"Error: {str(e)}")
        print("="*60 + "\n")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)