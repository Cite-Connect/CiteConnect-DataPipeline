"""
databias/bias_mitigation_collector.py
-------------------------------------
Reads collection recommendations from bias analysis and automatically
collects more papers from underrepresented fields to mitigate bias.
"""

import os
import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_collection_recommendations(recommendations_path: str = "/opt/airflow/databias_v2/slices/collection_recommendations.json") -> Dict:
    """
    Load collection recommendations from bias analysis.
    
    Args:
        recommendations_path: Path to collection_recommendations.json
        
    Returns:
        Dictionary with recommendations data
    """
    if not os.path.exists(recommendations_path):
        raise FileNotFoundError(f"Recommendations file not found: {recommendations_path}")
    
    with open(recommendations_path, 'r') as f:
        recommendations = json.load(f)
    
    logger.info(f"✅ Loaded recommendations from {recommendations_path}")
    logger.info(f"   Total underrepresented subdomains: {recommendations.get('total_underrepresented_subdomains', len(recommendations.get('recommendations', [])))}")
    logger.info(f"   Total papers needed: {recommendations.get('total_papers_needed', 0)}")
    
    return recommendations


def generate_mitigation_search_terms(recommendations: Dict, max_subdomains: int = 5) -> tuple:
    """
    Generate search terms for collecting papers from underrepresented subdomains.
    
    Args:
        recommendations: Recommendations data from bias analysis
        max_subdomains: Maximum number of underrepresented subdomains to target (default: 5)
        
    Returns:
        Tuple of (search_terms, papers_per_term_map, domain_configs) where:
        - search_terms: List of search terms to use for collection
        - papers_per_term_map: Dict mapping search term to number of papers needed
        - domain_configs: Dict mapping search term to (domain, subdomain) tuple
    """
    search_terms = []
    papers_per_term_map = {}
    domain_configs = {}  # Maps search_term -> (domain, subdomain)
    
    # Get high-priority subdomains first (sorted by priority and papers_needed)
    sorted_recs = sorted(
        recommendations['recommendations'],
        key=lambda x: (x['priority'] == 'high', x['papers_needed']),
        reverse=True
    )
    
    # Take top N subdomains
    for rec in sorted_recs[:max_subdomains]:
        subdomain = rec['subdomain']
        parent_domain = rec['parent_domain']
        queries = rec['suggested_search_queries']
        papers_needed = rec['papers_needed']
        papers_per_query = rec['papers_per_query']
        
        logger.info(f"\n📊 Subdomain: {subdomain}")
        logger.info(f"   Parent Domain: {parent_domain}")
        logger.info(f"   Priority: {rec['priority']}")
        logger.info(f"   Papers needed: {papers_needed}")
        logger.info(f"   Papers per query: {papers_per_query}")
        logger.info(f"   Queries to use: {len(queries)}")
        
        # Add primary query (subdomain itself) with domain context
        primary_query = subdomain
        search_terms.append(primary_query)
        papers_per_term_map[primary_query] = papers_needed  # Use full papers_needed for primary
        domain_configs[primary_query] = (parent_domain, subdomain)
    
    logger.info(f"\n✅ Generated {len(search_terms)} search terms for mitigation collection")
    
    return search_terms, papers_per_term_map, domain_configs


def run_mitigation_collection(
    domain_recommendations: List[Dict],
    raw_output_dir: str = "/tmp/mitigation_data/raw",
    processed_output_dir: str = "/tmp/mitigation_data/processed"
) -> Dict:
    """
    Run collection for underrepresented domains using iterative subdomain strategy.
    
    NEW APPROACH:
    - Bias analysis identifies underrepresented DOMAINS
    - For each domain, iterate through its subdomains as search terms
    - Keep collecting until papers_needed is reached or all subdomains exhausted
    
    Args:
        domain_recommendations: List of domain recommendations from bias analysis
                               Each contains: domain, papers_needed, available_subdomains
        raw_output_dir: Output directory for raw papers
        processed_output_dir: Output directory for processed papers
        
    Returns:
        Dictionary with collection results
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🚀 STARTING DOMAIN-BASED MITIGATION COLLECTION")
    logger.info(f"{'='*60}")
    logger.info(f"   Underrepresented domains: {len(domain_recommendations)}")
    
    # Import here to avoid circular dependencies
    from src.DataPipeline.Ingestion.main import collect_and_process_pipeline
    
    # Store original environment variables
    original_max_refs = os.environ.get('MAX_REFERENCES_PER_PAPER')
    original_is_mitigation = os.environ.get('IS_BIAS_MITIGATION')
    
    # Global results tracking
    all_collection_results = []
    all_processing_results = []
    domain_summaries = []
    
    try:
        # Set environment variables for mitigation collection
        os.environ['MAX_REFERENCES_PER_PAPER'] = '0'
        os.environ['IS_BIAS_MITIGATION'] = 'true'
        logger.info(f"   MAX_REFERENCES_PER_PAPER: 0 (no references for mitigation)")
        logger.info(f"   IS_BIAS_MITIGATION: true (files will have '_mitigated' suffix)")
        
        # Process each underrepresented domain
        for domain_rec in domain_recommendations:
            domain = domain_rec['domain']
            papers_needed = domain_rec['papers_needed']
            available_subdomains = domain_rec['available_subdomains']
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📂 DOMAIN: {domain}")
            logger.info(f"{'='*60}")
            logger.info(f"   Target papers: {papers_needed}")
            logger.info(f"   Available subdomains: {len(available_subdomains)}")
            logger.info(f"   Subdomain list: {', '.join(available_subdomains)}")
            
            # Set domain in environment
            os.environ['COLLECTION_DOMAIN'] = domain
            
            # Iteratively collect using subdomains until target reached
            papers_collected_for_domain = 0
            subdomains_used = []
            
            # Get max papers per query from environment (typically 100)
            max_papers_per_query = int(os.getenv('PAPERS_PER_TERM', '100'))
            logger.info(f"   Max papers per API query: {max_papers_per_query}")
            
            for idx, subdomain in enumerate(available_subdomains, 1):
                remaining = papers_needed - papers_collected_for_domain
                
                if remaining <= 0:
                    logger.info(f"\n✅ Target reached for {domain}! ({papers_collected_for_domain}/{papers_needed})")
                    break
                
                logger.info(f"\n   [{idx}/{len(available_subdomains)}] Trying subdomain: {subdomain}")
                logger.info(f"   Papers still needed: {remaining}")
                
                # Set subdomain in environment
                os.environ['COLLECTION_SUBDOMAINS'] = subdomain
                
                # If remaining > max_per_query, we need multiple collection runs
                # Collect in chunks to respect API limits
                papers_collected_this_subdomain = 0
                collection_round = 0
                
                while remaining > 0 and papers_collected_this_subdomain < remaining:
                    collection_round += 1
                    # Limit each collection to max_papers_per_query
                    this_round_limit = min(remaining - papers_collected_this_subdomain, max_papers_per_query)
                    
                    if collection_round > 1:
                        logger.info(f"      Round {collection_round}: Collecting {this_round_limit} more papers...")
                    
                    try:
                        # Run collection for this subdomain (respecting API limit)
                        results = collect_and_process_pipeline(
                            search_terms=[subdomain],
                            limit=this_round_limit,  # ✅ Respect API limit!
                            raw_output_dir=raw_output_dir,
                            processed_output_dir=processed_output_dir,
                            use_async=True
                        )
                        
                        # Extract results
                        collection_results = results.get('collection_results', [])
                        processing_results = results.get('processing_results', [])
                        
                        # Count papers collected in this round
                        papers_this_round = sum(r.get('paper_count', 0) for r in collection_results)
                        
                        if papers_this_round == 0:
                            logger.warning(f"      ⚠️ No papers returned in round {collection_round}. Moving to next subdomain.")
                            break  # No more papers available from this subdomain
                        
                        papers_collected_this_subdomain += papers_this_round
                        papers_collected_for_domain += papers_this_round
                        
                        logger.info(f"      ✅ Round {collection_round}: +{papers_this_round} papers")
                        
                        # Track results
                        all_collection_results.extend(collection_results)
                        all_processing_results.extend(processing_results)
                        
                        # If we collected less than requested, no more papers available
                        if papers_this_round < this_round_limit:
                            logger.info(f"      ℹ️ Subdomain exhausted (collected {papers_this_round}/{this_round_limit})")
                            break
                        
                    except Exception as e:
                        logger.warning(f"      ⚠️ Collection round {collection_round} failed: {e}")
                        break  # Stop trying this subdomain
                
                if papers_collected_this_subdomain > 0:
                    logger.info(f"   ✅ Total from '{subdomain}': {papers_collected_this_subdomain} papers")
                    logger.info(f"   📊 Domain progress: {papers_collected_for_domain}/{papers_needed}")
                    subdomains_used.append(subdomain)
                else:
                    logger.warning(f"   ⚠️ No papers collected from '{subdomain}'")
                    logger.info(f"   Continuing with next subdomain...")
            
            # Summary for this domain
            domain_summary = {
                'domain': domain,
                'papers_needed': papers_needed,
                'papers_collected': papers_collected_for_domain,
                'subdomains_used': subdomains_used,
                'subdomains_available': len(available_subdomains),
                'success_rate': (papers_collected_for_domain / papers_needed * 100) if papers_needed > 0 else 0
            }
            domain_summaries.append(domain_summary)
            
            if papers_collected_for_domain >= papers_needed:
                logger.info(f"\n✅ {domain}: TARGET REACHED ({papers_collected_for_domain}/{papers_needed})")
            else:
                logger.info(f"\n⚠️ {domain}: PARTIAL ({papers_collected_for_domain}/{papers_needed})")
                logger.info(f"   All {len(available_subdomains)} subdomains exhausted")
        
        # Calculate overall statistics
        total_papers_collected = sum(s['papers_collected'] for s in domain_summaries)
        total_papers_needed = sum(s['papers_needed'] for s in domain_summaries)
        total_papers_processed = sum(r.get('processed_count', 0) for r in all_processing_results)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ MITIGATION COLLECTION COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"   Total papers collected: {total_papers_collected}/{total_papers_needed}")
        logger.info(f"   Total papers processed: {total_papers_processed}")
        logger.info(f"   Domains processed: {len(domain_summaries)}")
        logger.info(f"\n   Domain-wise Summary:")
        for summary in domain_summaries:
            logger.info(f"      • {summary['domain']}: {summary['papers_collected']}/{summary['papers_needed']} "
                       f"({summary['success_rate']:.1f}%) via {len(summary['subdomains_used'])} subdomains")
        
        return {
            'status': 'success',
            'total_papers_collected': total_papers_collected,
            'total_papers_needed': total_papers_needed,
            'total_papers_processed': total_papers_processed,
            'domains_processed': len(domain_summaries),
            'domain_summaries': domain_summaries,
            'collection_results': all_collection_results,
            'processing_results': all_processing_results
        }
        
    except Exception as e:
        logger.error(f"❌ Mitigation collection failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'failed',
            'error': str(e)
        }
    finally:
        # Restore original environment variables
        if original_max_refs is not None:
            os.environ['MAX_REFERENCES_PER_PAPER'] = original_max_refs
        elif 'MAX_REFERENCES_PER_PAPER' in os.environ:
            del os.environ['MAX_REFERENCES_PER_PAPER']
            
        if original_is_mitigation is not None:
            os.environ['IS_BIAS_MITIGATION'] = original_is_mitigation
        elif 'IS_BIAS_MITIGATION' in os.environ:
            del os.environ['IS_BIAS_MITIGATION']
            
        logger.info(f"\n   Environment variables restored")


def save_mitigation_report(collection_results: Dict, output_path: str = "/opt/airflow/databias_v2/slices/mitigation_report.json"):
    """
    Save mitigation collection report to GCS (primary) and local (cache).
    
    Args:
        collection_results: Results from run_mitigation_collection (domain-based)
        output_path: Local path to save report (for inter-task communication)
    """
    import pandas as pd
    
    report = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'status': collection_results['status'],
        'total_papers_collected': collection_results.get('total_papers_collected', 0),
        'total_papers_needed': collection_results.get('total_papers_needed', 0),
        'total_papers_processed': collection_results.get('total_papers_processed', 0),
        'domains_processed': collection_results.get('domains_processed', 0),
        'domain_summaries': collection_results.get('domain_summaries', []),
    }
    
    if collection_results['status'] == 'failed':
        report['error'] = collection_results.get('error', 'Unknown error')
    
    # Save to local cache for inter-task communication
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    logger.info(f"💾 Mitigation report cached locally: {output_path}")
    
    # Upload to GCS (primary storage)
    try:
        from google.cloud import storage
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("databias_v2/slices/mitigation_report.json")
        blob.upload_from_string(json.dumps(report, indent=2), content_type='application/json')
        logger.info(f"📤 Mitigation report uploaded to GCS: gs://{bucket_name}/databias_v2/slices/mitigation_report.json")
    except Exception as e:
        logger.warning(f"⚠️ Could not upload to GCS: {e}")


def recheck_bias_after_mitigation():
    """
    Re-run bias analysis after collecting mitigation papers.
    This will include the newly collected papers in the analysis.
    """
    import subprocess
    
    logger.info("\n🔄 Re-running bias analysis with new papers...")
    
    try:
        result = subprocess.run(
            ["python", "databias/slicing_bias_analysis.py"],
            capture_output=True,
            text=True,
            timeout=600
        )
        
        logger.info(result.stdout)
        if result.stderr:
            logger.warning(result.stderr)
        
        if result.returncode == 0:
            logger.info("✅ Bias re-analysis completed successfully")
            return {'status': 'success'}
        else:
            logger.error(f"❌ Bias re-analysis failed with exit code: {result.returncode}")
            return {'status': 'failed', 'exit_code': result.returncode}
            
    except Exception as e:
        logger.error(f"❌ Error during bias re-analysis: {e}")
        return {'status': 'error', 'error': str(e)}


def run_full_mitigation_cycle(
    max_fields: int = 5,
    recheck_bias: bool = True,
    dag_context: Dict = None
) -> Dict:
    """
    Complete mitigation cycle (DOMAIN-BASED):
    1. Read domain-based recommendations from bias analysis
    2. For each underrepresented domain, iteratively collect using its subdomains
    3. Re-run bias analysis
    
    Args:
        max_fields: Maximum number of underrepresented domains to target
        recheck_bias: Whether to re-run bias analysis after collection
        dag_context: Optional Airflow DAG context for XCom
        
    Returns:
        Dictionary with complete cycle results
    """
    logger.info("="*60)
    logger.info("BIAS MITIGATION CYCLE START (DOMAIN-BASED)")
    logger.info("="*60)
    
    try:
        # Step 1: Load domain-based recommendations
        recommendations_data = load_collection_recommendations()
        
        # Extract top N domain recommendations
        domain_recommendations = recommendations_data.get('recommendations', [])[:max_fields]
        
        if not domain_recommendations:
            logger.warning("⚠️ No domain recommendations found. Skipping collection.")
            return {'status': 'skipped', 'reason': 'no_recommendations'}
        
        logger.info(f"   Targeting {len(domain_recommendations)} underrepresented domains")
        for rec in domain_recommendations:
            logger.info(f"      • {rec['domain']}: needs {rec['papers_needed']} papers, "
                       f"{len(rec['available_subdomains'])} subdomains available")
        
        # Step 2: Run domain-based mitigation collection
        # This will iterate through subdomains for each domain
        collection_results = run_mitigation_collection(
            domain_recommendations=domain_recommendations
        )
        
        # Step 3: Save report
        save_mitigation_report(collection_results)
        
        # Step 4: Re-check bias (optional)
        recheck_results = None
        if recheck_bias and collection_results['status'] == 'success':
            logger.info("\n🔄 Re-checking bias after mitigation...")
            recheck_results = recheck_bias_after_mitigation()
        
        logger.info("\n" + "="*60)
        logger.info("BIAS MITIGATION CYCLE COMPLETE")
        logger.info("="*60)
        
        final_result = {
            'status': 'success',
            'collection_results': collection_results,
            'recheck_results': recheck_results
        }
        
        # Push to XCom if in DAG context
        if dag_context:
            ti = dag_context.get('ti')
            if ti:
                ti.xcom_push(key='mitigation_results', value=final_result)
        
        return final_result
        
    except Exception as e:
        logger.error(f"❌ Mitigation cycle failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'failed',
            'error': str(e)
        }


# Main entry point for CLI/DAG execution
if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    max_fields = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    result = run_full_mitigation_cycle(
        max_fields=max_fields,
        recheck_bias=True
    )
    
    sys.exit(0 if result['status'] == 'success' else 1)

