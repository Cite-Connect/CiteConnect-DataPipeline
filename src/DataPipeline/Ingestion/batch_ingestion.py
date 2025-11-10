"""
Batch Ingestion Script for CiteConnect
Runs multiple search queries sequentially with progress tracking
"""

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_ingestion(search_term, papers_per_term=100, output_dir="data/raw"):
    """
    Run ingestion.py for a single search term
    
    Args:
        search_term: Search query string
        papers_per_term: Number of papers to collect
        output_dir: Output directory for parquet files
    
    Returns:
        True if successful, False if failed
    """
    cmd = [
        sys.executable,
        "ingestion.py",
        search_term,
        "--limit", str(papers_per_term),
        "--output", output_dir
    ]
    
    logging.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per query
        )
        
        if result.returncode == 0:
            logging.info(f"SUCCESS: {search_term}")
            return True
        else:
            logging.error(f"FAILED: {search_term}")
            logging.error(f"Error output: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"TIMEOUT: {search_term} (exceeded 1 hour)")
        return False
    except Exception as e:
        logging.error(f"ERROR: {search_term} - {str(e)}")
        return False


def run_batch_ingestion(topics, papers_per_term=100, output_dir="data/raw", 
                       delay_between_topics=30):
    """
    Run ingestion for multiple topics sequentially
    
    Args:
        topics: List of search terms
        papers_per_term: Papers to collect per term
        output_dir: Output directory
        delay_between_topics: Seconds to wait between topics
    """
    
    start_time = datetime.now()
    total_topics = len(topics)
    
    logging.info("="*60)
    logging.info("BATCH INGESTION STARTED")
    logging.info("="*60)
    logging.info(f"Total topics: {total_topics}")
    logging.info(f"Papers per topic: {papers_per_term}")
    logging.info(f"Expected total papers: {total_topics * papers_per_term}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("="*60 + "\n")
    
    results = {
        'successful': [],
        'failed': [],
    }
    
    for idx, topic in enumerate(topics, 1):
        logging.info(f"\nTopic {idx}/{total_topics}: {topic}")
        logging.info("-"*60)
        
        topic_start = datetime.now()
        success = run_ingestion(topic, papers_per_term, output_dir)
        topic_end = datetime.now()
        topic_duration = (topic_end - topic_start).total_seconds()
        
        if success:
            results['successful'].append({
                'topic': topic,
                'duration_seconds': topic_duration
            })
            logging.info(f"Completed in {topic_duration:.0f} seconds")
        else:
            results['failed'].append(topic)
        
        # Wait between topics (except after last one)
        if idx < total_topics:
            logging.info(f"Waiting {delay_between_topics}s before next topic...")
            time.sleep(delay_between_topics)
    
    # Final summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    
    logging.info("\n" + "="*60)
    logging.info("BATCH INGESTION COMPLETE")
    logging.info("="*60)
    logging.info(f"Total time: {total_duration/60:.1f} minutes")
    logging.info(f"Successful: {len(results['successful'])}/{total_topics}")
    logging.info(f"Failed: {len(results['failed'])}/{total_topics}")
    
    if results['successful']:
        avg_time = sum(r['duration_seconds'] for r in results['successful']) / len(results['successful'])
        logging.info(f"Average time per topic: {avg_time/60:.1f} minutes")
    
    if results['failed']:
        logging.warning(f"\nFailed topics:")
        for topic in results['failed']:
            logging.warning(f"  - {topic}")
    
    logging.info("="*60)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Batch ingestion runner for multiple search queries'
    )
    
    parser.add_argument(
        'topics',
        nargs='+',
        help='Search topics (space-separated, use quotes for multi-word topics)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Papers per topic (default: 100)'
    )
    
    parser.add_argument(
        '--output',
        default='data/raw',
        help='Output directory (default: data/raw)'
    )
    
    parser.add_argument(
        '--delay',
        type=int,
        default=30,
        help='Seconds to wait between topics (default: 30)'
    )
    
    args = parser.parse_args()
    
    # Run batch ingestion
    results = run_batch_ingestion(
        topics=args.topics,
        papers_per_term=args.limit,
        output_dir=args.output,
        delay_between_topics=args.delay
    )
    
    # Exit with error code if any failed
    if results['failed']:
        sys.exit(1)
    else:
        sys.exit(0)
"""
Batch Ingestion Script for CiteConnect
Runs multiple search queries sequentially with progress tracking
"""

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Healthcare AI subtopics
HEALTHCARE_TOPICS = [
    "medical image classification deep learning",
    "clinical NLP medical text mining",
    "disease prediction diagnosis machine learning",
    "drug discovery deep learning molecular design",
    "medical image segmentation U-Net CNN",
    "electronic health records prediction patient outcomes",
    "radiology report generation AI",
    "digital pathology histopathology deep learning",
    "genomics precision medicine AI",
    "medical question answering chatbot healthcare",
]

def run_ingestion(search_term, papers_per_term=100, output_dir="data/raw"):
    """
    Run ingestion.py for a single search term
    
    Args:
        search_term: Search query string
        papers_per_term: Number of papers to collect
        output_dir: Output directory for parquet files
    
    Returns:
        True if successful, False if failed
    """
    cmd = [
        sys.executable,
        "ingestion.py",
        search_term,
        "--limit", str(papers_per_term),
        "--output", output_dir
    ]
    
    logging.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per query
        )
        
        if result.returncode == 0:
            logging.info(f"SUCCESS: {search_term}")
            return True
        else:
            logging.error(f"FAILED: {search_term}")
            logging.error(f"Error output: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"TIMEOUT: {search_term} (exceeded 1 hour)")
        return False
    except Exception as e:
        logging.error(f"ERROR: {search_term} - {str(e)}")
        return False


def run_batch_ingestion(topics, papers_per_term=100, output_dir="data/raw", 
                       delay_between_topics=30):
    """
    Run ingestion for multiple topics sequentially
    
    Args:
        topics: List of search terms
        papers_per_term: Papers to collect per term
        output_dir: Output directory
        delay_between_topics: Seconds to wait between topics
    """
    
    start_time = datetime.now()
    total_topics = len(topics)
    
    logging.info("="*60)
    logging.info("BATCH INGESTION STARTED")
    logging.info("="*60)
    logging.info(f"Total topics: {total_topics}")
    logging.info(f"Papers per topic: {papers_per_term}")
    logging.info(f"Expected total papers: {total_topics * papers_per_term}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("="*60 + "\n")
    
    results = {
        'successful': [],
        'failed': [],
    }
    
    for idx, topic in enumerate(topics, 1):
        logging.info(f"\nTopic {idx}/{total_topics}: {topic}")
        logging.info("-"*60)
        
        topic_start = datetime.now()
        success = run_ingestion(topic, papers_per_term, output_dir)
        topic_end = datetime.now()
        topic_duration = (topic_end - topic_start).total_seconds()
        
        if success:
            results['successful'].append({
                'topic': topic,
                'duration_seconds': topic_duration
            })
            logging.info(f"Completed in {topic_duration:.0f} seconds")
        else:
            results['failed'].append(topic)
        
        # Wait between topics (except after last one)
        if idx < total_topics:
            logging.info(f"Waiting {delay_between_topics}s before next topic...")
            time.sleep(delay_between_topics)
    
    # Final summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    
    logging.info("\n" + "="*60)
    logging.info("BATCH INGESTION COMPLETE")
    logging.info("="*60)
    logging.info(f"Total time: {total_duration/60:.1f} minutes")
    logging.info(f"Successful: {len(results['successful'])}/{total_topics}")
    logging.info(f"Failed: {len(results['failed'])}/{total_topics}")
    
    if results['successful']:
        avg_time = sum(r['duration_seconds'] for r in results['successful']) / len(results['successful'])
        logging.info(f"Average time per topic: {avg_time/60:.1f} minutes")
    
    if results['failed']:
        logging.warning(f"\nFailed topics:")
        for topic in results['failed']:
            logging.warning(f"  - {topic}")
    
    logging.info("="*60)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Batch ingestion runner for multiple search queries'
    )
    
    parser.add_argument(
        'topics',
        nargs='*',
        help='Search topics (space-separated). If not provided, uses default healthcare topics.'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Papers per topic (default: 100)'
    )
    
    parser.add_argument(
        '--output',
        default='data/raw',
        help='Output directory (default: data/raw)'
    )
    
    parser.add_argument(
        '--delay',
        type=int,
        default=30,
        help='Seconds to wait between topics (default: 30)'
    )
    
    parser.add_argument(
        '--use-defaults',
        action='store_true',
        help='Use default 10 healthcare topics'
    )
    
    args = parser.parse_args()
    
    # Determine which topics to use
    if args.use_defaults or not args.topics:
        topics_to_run = HEALTHCARE_TOPICS
        logging.info("Using default 10 healthcare AI topics")
    else:
        topics_to_run = args.topics
    
    # Run batch ingestion
    results = run_batch_ingestion(
        topics=topics_to_run,
        papers_per_term=args.limit,
        output_dir=args.output,
        delay_between_topics=args.delay
    )
    
    # Exit with error code if any failed
    if results['failed']:
        sys.exit(1)
    else:
        sys.exit(0)