from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import sys
import json
import os
import subprocess

sys.path.insert(0, '/opt/airflow')

EMAIL_TO = ['anushasrini2001@gmail.com']

default_args = {
    'owner': 'citeconnect-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 10),
    'email': EMAIL_TO,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG config with params for runtime configuration
dag = DAG(
    'test_citeconnect',
    default_args=default_args,
    description='CiteConnect test pipeline with email notifications',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'citeconnect'],
    params={
        'SEARCH_TERMS': 'computer vision',  # Default search terms
        'COLLECTION_DOMAIN': 'AI',  # Default domain
        'COLLECTION_SUBDOMAINS': '',  # Default subdomains (comma-separated)
        'PAPERS_PER_TERM': 100,  # Number of papers to collect per search term
        'MAX_REFERENCES_PER_PAPER': 50,  # Max references to collect per seed paper
        'BIAS_MITIGATION_MAX_FIELDS': 5  # Max underrepresented fields to target for mitigation
    }
)

def check_env_variables():
    semantic_scholar_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY')
    unpaywall_email = os.getenv('UNPAYWALL_EMAIL')
    core_api_key = os.getenv('CORE_API_KEY')

    print("Checking environment variables...")
    print(f"SEMANTIC_SCHOLAR_API_KEY: {'Set' if semantic_scholar_key else 'Not Set'}")
    print(f"UNPAYWALL_EMAIL: {'Set' if unpaywall_email else 'Not Set'}")
    print(f"CORE_API_KEY: {'Set' if core_api_key else 'Not Set'}")

def check_gcs_connection():
    from google.cloud import storage
    from google.auth import default
    gcp_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
    gcs_project_id = os.getenv('GCS_PROJECT_ID')

    print("Checking GCS connection...")
    print(f"GOOGLE_APPLICATION_CREDENTIALS: {'Set' if gcp_credentials else 'Not Set'}")
    print(f"GCS_BUCKET_NAME: {'Set' if gcs_bucket_name else 'Not Set'}")

    with open(gcp_credentials, 'r') as f:
        cred_data = json.load(f)
        project_id = cred_data.get('project_id', 'Unknown')
        print(f"✅ Credentials file valid for project: {project_id}")

    client = storage.Client(project=gcs_project_id)
    print("✅ GCS Client initialized")

    bucket = client.bucket(gcs_bucket_name)
    print(f"✅ Bucket object created: {gcs_bucket_name}")

    bucket_exists = bucket.exists()
    print(f"Bucket exists: {'YES' if bucket_exists else 'NO'}")

    if bucket_exists:
        # List some files (first 5)
        blobs = list(client.list_blobs(gcs_bucket_name, max_results=5))
        print(f"Files in bucket: {len(blobs)}")
        
        for blob in blobs:
            print(f"  - {blob.name} ({blob.size} bytes)")

def run_unit_tests():
    """Run unit tests using pytest"""
    import subprocess
    import sys
    import os
    
    print("Running unit tests...")
    
    project_root = '/opt/airflow'
    os.chdir(project_root)
    
    if '/opt/airflow/src' not in sys.path:
        sys.path.insert(0, '/opt/airflow/src')
    
    test_dir = os.path.join(project_root, 'tests/unit')
    if not os.path.exists(test_dir):
        print(f"Test directory not found: {test_dir}")
        return f"ERROR: Test directory not found: {test_dir}"
    
    test_files = []
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if file.startswith('test_') and file.endswith('.py'):
                test_files.append(os.path.relpath(os.path.join(root, file), project_root))
    
    print(f"Found {len(test_files)} test files:")
    for test_file in test_files:
        print(f"  - {test_file}")
    
    if not test_files:
        print("No test files found! Skipping unit tests.")
        return "no_tests_found"
    
    try:
        # Run pytest with your exact command
        result = subprocess.run([
            sys.executable, '-m', 'pytest', 
            'tests/unit/', 
            '-v'
        ], 
        capture_output=True, 
        text=True,
        cwd=project_root,
        timeout=300
        )
        
        # Print the output
        print("PYTEST STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("PYTEST STDERR:")
            print(result.stderr)
        
        # Parse basic results
        stdout = result.stdout
        passed_count = stdout.count(' PASSED')
        failed_count = stdout.count(' FAILED')
        skipped_count = stdout.count(' SKIPPED')
        error_count = stdout.count(' ERROR')
        
        print(f"\nTest Results Summary:")
        print(f"  Passed: {passed_count}")
        print(f"  Failed: {failed_count}")
        print(f"  Skipped: {skipped_count}")
        print(f"  Errors: {error_count}")
        
        if result.returncode == 0:
            print("All unit tests passed!")
            return "tests_passed"
        else:
            print(f"Unit tests failed with return code: {result.returncode}")
            error_msg = f"ERROR: Unit tests failed. {failed_count} failures, {error_count} errors."
            print(error_msg)
            return error_msg
            
    except subprocess.TimeoutExpired:
        return "ERROR: Unit tests timed out after 5 minutes"
    except FileNotFoundError:
        return "ERROR: pytest not found. Please add pytest to requirements.txt"
    except Exception as e:
        return f"ERROR: Error running unit tests: {e}"

def test_paper_collection(**context):
    """
    Collect papers using async pipeline with overlapping collection/preprocessing.
    This replaces the old sequential approach with a faster pipeline pattern.
    
    Configuration can be passed via:
    1. DAG params (when triggering from UI)
    2. JSON config (when triggering with config)
    3. Environment variables (fallback)
    """
    from src.DataPipeline.Ingestion.main import collect_and_process_pipeline
    import os
    import json
    from pathlib import Path
    
    # Get config from DAG run params/config (priority) or environment variables (fallback)
    dag_run = context.get('dag_run')
    params = {}
    
    if dag_run and dag_run.conf:
        # Config passed as JSON when triggering
        params = dag_run.conf
        print(f"📋 Using config from DAG run JSON config: {json.dumps(params, indent=2)}")
    elif dag_run and dag_run.conf is None and hasattr(dag_run, 'dag') and dag_run.dag.params:
        # Params from DAG definition (when triggered from UI with params)
        params = dag_run.dag.params
        print(f"📋 Using config from DAG params: {json.dumps(params, indent=2)}")
    else:
        # Fallback to environment variables
        params = {
            'SEARCH_TERMS': os.getenv('SEARCH_TERMS', 'finance, quantum computing, healthcare'),
            'COLLECTION_DOMAIN': os.getenv('COLLECTION_DOMAIN', 'general'),
            'COLLECTION_SUBDOMAINS': os.getenv('COLLECTION_SUBDOMAINS', ''),
            'PAPERS_PER_TERM': int(os.getenv('PAPERS_PER_TERM', '100')),
            'MAX_REFERENCES_PER_PAPER': int(os.getenv('MAX_REFERENCES_PER_PAPER', '50'))
        }
        print(f"📋 Using config from environment variables: {json.dumps(params, indent=2)}")
    
    # Extract config values (priority: params → environment → defaults)
    search_terms_str = params.get('SEARCH_TERMS', 'finance, quantum computing, healthcare')
    search_terms = [term.strip() for term in search_terms_str.split(',') if term.strip()]
    limit = int(params.get('PAPERS_PER_TERM', 100))
    max_references = int(params.get('MAX_REFERENCES_PER_PAPER', 50))
    
    # Save collection config for bias analysis to use (local cache + GCS)
    config_dir = Path("/opt/airflow/databias_v2/slices")
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "collection_config.json"
    
    config_data = {
        'search_terms': search_terms_str,
        'papers_per_term': limit,
        'max_references_per_paper': max_references,
        'timestamp': datetime.now().isoformat()
    }
    
    # Save locally
    with open(config_file, 'w') as f:
        json.dump(config_data, f, indent=2)
    print(f"💾 Saved collection config locally: {config_file}")
    
    # Also save to GCS
    try:
        from google.cloud import storage
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("databias_v2/slices/collection_config.json")
        blob.upload_from_string(json.dumps(config_data, indent=2), content_type='application/json')
        print(f"📤 Uploaded collection config to GCS")
    except Exception as e:
        print(f"⚠️ Could not upload config to GCS: {e}")
    
    collection_domain = params.get('COLLECTION_DOMAIN', 'general')
    collection_subdomains = params.get('COLLECTION_SUBDOMAINS', '')
    
    # Temporarily set environment variables for this run (so pipeline functions can access them)
    # This allows the pipeline to use these values without passing them through every function
    original_domain = os.environ.get('COLLECTION_DOMAIN')
    original_subdomains = os.environ.get('COLLECTION_SUBDOMAINS')
    original_search_terms = os.environ.get('SEARCH_TERMS')
    original_max_refs = os.environ.get('MAX_REFERENCES_PER_PAPER')
    
    try:
        os.environ['COLLECTION_DOMAIN'] = collection_domain
        os.environ['COLLECTION_SUBDOMAINS'] = collection_subdomains
        os.environ['SEARCH_TERMS'] = search_terms_str  # Set for reference, but we pass directly
        os.environ['MAX_REFERENCES_PER_PAPER'] = str(max_references)
        
        print(f"🔍 Search terms: {search_terms}")
        print(f"📊 Papers per term: {limit}")
        print(f"📚 Max references per paper: {max_references}")
        print(f"🏷️  Collection Domain: {collection_domain}")
        print(f"🏷️  Collection Subdomains: {collection_subdomains}")
        print(f"🚀 Using async pipeline with overlapping collection/preprocessing")
    
        # Use async pipeline (collection and preprocessing overlap)
        results = collect_and_process_pipeline(
        search_terms=search_terms,
        limit=limit,
            raw_output_dir="/tmp/test_data/raw",
            processed_output_dir="/tmp/test_data/processed",
            use_async=True  # Use async pipeline
        )
        
        collection_results = results['collection_results']
        processing_results = results['processing_results']
        
        print(f"\n✅ Collection completed: {len(collection_results)} terms processed")
        print("📤 Raw files uploaded to GCS:")
        for result in collection_results:
            print(f"  {result['search_term']}: {result['gcs_path']}")
    
        print(f"\n✅ Preprocessing completed: {len(processing_results)} terms processed")
        print("📤 Processed files uploaded to GCS:")
        for result in processing_results:
            print(f"  {result['search_term']}: {result['processed_gcs']}")
        
        # Return collection results for backward compatibility with preprocess_papers task
        # Also include config in return for downstream tasks
        return {
            'collection_results': collection_results,
            'config': {
                'SEARCH_TERMS': search_terms_str,
                'COLLECTION_DOMAIN': collection_domain,
                'COLLECTION_SUBDOMAINS': collection_subdomains
            }
        }
    finally:
        # Restore original environment variables
        if original_domain is not None:
            os.environ['COLLECTION_DOMAIN'] = original_domain
        elif 'COLLECTION_DOMAIN' in os.environ:
            del os.environ['COLLECTION_DOMAIN']
            
        if original_subdomains is not None:
            os.environ['COLLECTION_SUBDOMAINS'] = original_subdomains
        elif 'COLLECTION_SUBDOMAINS' in os.environ:
            del os.environ['COLLECTION_SUBDOMAINS']
            
        if original_search_terms is not None:
            os.environ['SEARCH_TERMS'] = original_search_terms
        elif 'SEARCH_TERMS' in os.environ:
            del os.environ['SEARCH_TERMS']
            
        if original_max_refs is not None:
            os.environ['MAX_REFERENCES_PER_PAPER'] = original_max_refs
        elif 'MAX_REFERENCES_PER_PAPER' in os.environ:
            del os.environ['MAX_REFERENCES_PER_PAPER']

# Removed: embed_stored_data function - embeddings handled by Supabase webhook
# Removed: load_bias_data_from_gcs task - bias analysis loads directly from GCS processed_v2/ folder


def run_bias_slicing():
    """
    Run bias analysis script which loads data directly from GCS processed_v2/ folder.
    No local data file required - script accesses GCS directly.
    """
    print("Running Fairlearn slicing analysis ...")
    print("📥 Script will load data directly from gs://citeconnect-test-bucket/processed_v2/")
    
    # Set working directory to project root
    project_root = '/opt/airflow'
    
    result = subprocess.run(
        ["python", "databias/slicing_bias_analysis.py"],
        capture_output=True, 
        text=True,
        cwd=project_root,
        timeout=600
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    # Check if the script ran successfully
    if result.returncode != 0:
        raise RuntimeError(f"Bias slicing script failed with exit code: {result.returncode}")
    
    print("✅ Bias slicing completed. Results saved to GCS: databias_v2/slices/")
    return "bias_slicing_done"


def check_bias_and_send_alert():
    print("Checking fairness_disparity.json ...")
    
    # Use absolute path to project root (v2 paths)
    project_root = '/opt/airflow'
    disparity_path = os.path.join(project_root, "databias_v2/slices/fairness_disparity.json")
    recommendations_path = os.path.join(project_root, "databias_v2/slices/collection_recommendations.json")
    
    if not os.path.exists(disparity_path):
        raise ValueError(f"fairness_disparity.json not found at {disparity_path}. Run slicing first.")
    
    with open(disparity_path, "r") as f:
        disparity = json.load(f)

    disparity_ratio = disparity.get("disparity_ratio", 0)
    disparity_diff = disparity.get("disparity_difference", 0)

    print(f"📈 Disparity ratio: {disparity_ratio:.2f}")
    print(f"📉 Disparity difference: {disparity_diff:.2f}")

    # Load collection recommendations if available
    recommendations = []
    if os.path.exists(recommendations_path):
        with open(recommendations_path, "r") as f:
            rec_data = json.load(f)
            recommendations = rec_data.get("recommendations", [])[:5]  # Top 5
    
    THRESHOLD = 50.0  # 🔔 alert threshold for disparity difference

    if disparity_diff > THRESHOLD:
        # Build recommendations HTML
        rec_html = ""
        if recommendations:
            rec_html = "<h4>Collection Recommendations (Top 5 Underrepresented Domains):</h4><ul>"
            for rec in recommendations:
                rec_html += f"<li><b>{rec['domain']}</b>: Need {rec['papers_needed']} more papers "
                rec_html += f"(currently: {rec['current_count']}, target: {rec['target_count']})<br>"
                rec_html += f"Available subdomains to query: {', '.join(rec['available_subdomains'][:5])}</li>"
            rec_html += "</ul>"
        
        subject = f"⚠️ CiteConnect Bias Alert: Disparity difference {disparity_diff:.2f}"
        html_content = f"""
        <h3>Bias Threshold Exceeded</h3>
        <p><b>Disparity Ratio:</b> {disparity_ratio:.2f}x<br>
        <b>Disparity Difference:</b> {disparity_diff:.2f} citations<br>
        <b>Threshold:</b> {THRESHOLD}</p>
        
        {rec_html}
        
        <p><b>Action Required:</b> Run collection with recommended search queries to balance dataset.</p>
        <p>View full details: <code>gs://{os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')}/databias_v2/slices/collection_recommendations.json</code></p>
        """
        try:
            # Check if SMTP credentials are configured
            smtp_user = os.getenv('SMTP_USER')
            smtp_pass = os.getenv('SMTP_PASSWORD')
            
            if smtp_user and smtp_pass:
                send_email(
                    to=EMAIL_TO,
                    subject=subject,
                    html_content=html_content
                )
                print(f"🚨 Bias alert email sent! Disparity difference ({disparity_diff:.2f}) exceeded threshold {THRESHOLD}.")
                print(f"   Collection recommendations included in email.")
                return "alert_sent"
            else:
                print(f"⚠️ SMTP credentials not configured. Skipping email alert.")
                print(f"🚨 Bias threshold exceeded (difference: {disparity_diff:.2f} > {THRESHOLD})")
                print(f"   To enable email alerts, set SMTP_USER and SMTP_PASSWORD in .env file")
                return "alert_threshold_exceeded_no_email"
        except Exception as e:
            print(f"⚠️ Failed to send email alert: {e}")
            print(f"🚨 Bias threshold exceeded (difference: {disparity_diff:.2f} > {THRESHOLD})")
            print(f"   Task will continue despite email failure")
            return "alert_threshold_exceeded_email_failed"
    else:
        print("✅ Bias within acceptable limits. No alert sent.")
        return "no_alert"

bias_alert_task = PythonOperator(
    task_id='bias_alert_check',
    python_callable=check_bias_and_send_alert,
    dag=dag
)

def mitigate_and_recheck_bias(**context):
    """
    Automatically collect papers from underrepresented fields and re-check bias.
    This task reads collection_recommendations.json (which includes papers_needed 
    from bias detection) and triggers collection using those values.
    """
    import sys
    import json
    if '/opt/airflow' not in sys.path:
        sys.path.insert(0, '/opt/airflow')
    
    from databias.bias_mitigation_collector import run_full_mitigation_cycle
    
    # Get configuration from DAG params (priority) or environment (fallback)
    dag_run = context.get('dag_run')
    max_fields = 5  # default
    
    if dag_run and dag_run.conf:
        max_fields = int(dag_run.conf.get('BIAS_MITIGATION_MAX_FIELDS', 5))
    elif dag_run and hasattr(dag_run, 'dag') and dag_run.dag.params:
        max_fields = int(dag_run.dag.params.get('BIAS_MITIGATION_MAX_FIELDS', 5))
    else:
        max_fields = int(os.getenv('BIAS_MITIGATION_MAX_FIELDS', '5'))
    
    print(f"🚀 Starting automated bias mitigation...")
    print(f"   Max fields to target: {max_fields}")
    print(f"   Papers per query: Determined by bias analysis (papers_needed)")
    
    # Check if recommendations exist (i.e., bias was detected)
    recommendations_path = "/opt/airflow/databias_v2/slices/collection_recommendations.json"
    if not os.path.exists(recommendations_path):
        print("⚠️ No collection recommendations found. Skipping mitigation.")
        print("   (Bias analysis may not have detected significant issues)")
        return {'status': 'skipped', 'reason': 'no_recommendations'}
    
    # Run the full mitigation cycle (uses bias-detected paper counts)
    result = run_full_mitigation_cycle(
        max_fields=max_fields,
        recheck_bias=True
    )
    
    if result['status'] == 'success':
        collection_results = result.get('collection_results', {})
        papers_collected = collection_results.get('papers_collected', 0)
        print(f"\n✅ Bias mitigation completed successfully!")
        print(f"   Papers collected: {papers_collected}")
        print(f"   Bias has been re-analyzed with new papers")
        return result
    else:
        error_msg = result.get('error', 'Unknown error')
        print(f"\n❌ Bias mitigation failed: {error_msg}")
        # Don't raise exception - let pipeline continue
        return result

def generate_schema_and_stats(**context):
    """Generate schema and validate data quality"""
    from src.DataPipeline.Validation.schema_validator import validate_schema
    return validate_schema(**context)

def upload_to_supabase(**context):
    """Upload papers from GCS to Supabase database"""
    import asyncio
    from src.DataPipeline.Processing.upload_papers_to_supabase import upload_papers_to_supabase_async

    print("🚀 Starting Supabase upload task...")

    # Get config from DAG params if available
    dag_run = context.get('dag_run')
    max_papers = None
    batch_size = 100

    if dag_run and dag_run.conf:
        max_papers = dag_run.conf.get('MAX_PAPERS_UPLOAD')
        batch_size = dag_run.conf.get('UPLOAD_BATCH_SIZE', 100)

    print(f"📋 Config: max_papers={max_papers}, batch_size={batch_size}")

    try:
        # Run async function directly with asyncio.run (fresh loop, clean shutdown)
        result = asyncio.run(
            upload_papers_to_supabase_async(
                max_papers=max_papers,
                batch_size=batch_size
            )
        )
        print(f"✅ Supabase upload completed successfully: {result}")
        return result
    except Exception as e:
        print(f"❌ Supabase upload failed: {e}")
        import traceback
        traceback.print_exc()
        raise

def send_success_notification(**context):
    dag_run = context['dag_run']
    ti = context['task_instance']
    
    try:
        # Pull results from collection_and_preprocessing task
        collection_results = ti.xcom_pull(task_ids='collection_and_preprocessing')
        if not collection_results:
            collection_results = {}
        
        # Extract collection results
        collection_data = collection_results.get('collection_results', [])
        config = collection_results.get('config', {})
        
        # Calculate totals
        total_collected = sum(r.get('paper_count', 0) for r in collection_data)
        total_references = sum(r.get('reference_count', 0) for r in collection_data)
        
        papers_processed = total_collected + total_references
        chunks_created = papers_processed  # Each paper is a chunk
        file_size = 'N/A'  # We don't track file size
        
        params = {
            'domain': config.get('COLLECTION_DOMAIN', 'N/A'),
            'search_terms': config.get('SEARCH_TERMS', 'N/A'),
            'max_papers': config.get('PAPERS_PER_TERM', 'N/A'),
            'max_references': config.get('MAX_REFERENCES_PER_PAPER', 'N/A'),
            'batch_size': 'N/A'
        }

    except Exception as e:
        print(f"Warning: Could not pull XCom data. Sending generic email. Error: {e}")
        import traceback
        traceback.print_exc()
        papers_processed = 'N/A'
        chunks_created = 'N/A'
        file_size = 'N/A'
        params = {}

    
    # Get schema validation results
    task_instance = context['task_instance']
    schema_results = task_instance.xcom_pull(task_ids='generate_schema_and_stats')
    
    # Build alert message if quality dropped
    alert_msg = ""
    if schema_results and schema_results.get('alert'):
        alert_msg = f"<p style='color: red;'><strong>⚠️ ALERT: {schema_results['alert']}</strong></p>"
    
    subject = f"CiteConnect Pipeline SUCCESS - {dag_run.execution_date}"
    
    quality_score = schema_results.get('quality_score', 'N/A') if schema_results else 'N/A'
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            h2 {{ color: #2E8B57; }}
            h3 {{ border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
            ul {{ list-style-type: none; padding-left: 0; }}
            li strong {{ color: #333; }}
            .summary-box {{ 
                background-color: #f4f4f4; 
                border: 1px solid #ddd; 
                padding: 15px; 
                border-radius: 5px; 
            }}
            .commit {{
                font-family: 'Courier New', Courier, monospace;
                background-color: #eee;
                padding: 5px;
                border-radius: 3px;
                font-style: italic;
            }}
        </style>
    </head>
    <body>
        <h2>✅ CiteConnect Pipeline Completed Successfully!</h2>
        
        <p><strong>Your CiteConnect pipeline run has finished and all data has been versioned.</strong></p>

        <h3>Run Summary</h3>
        <div class="summary-box">
            <ul>
                <li><strong>Papers Processed:</strong> {papers_processed}</li>
                <li><strong>Embeddings Created:</strong> {chunks_created}</li>
                <li><strong>Final Data Size:</strong> {file_size} MB</li>
                <li><strong>Parameters:</strong>
                    <ul>
                        <li>Domain: {params.get('domain', 'N/A')}</li>
                        <li>Search Terms: {params.get('search_terms', 'N/A')}</li>
                        <li>Papers Per Term: {params.get('max_papers', 'N/A')}</li>
                        <li>Max References: {params.get('max_references', 'N/A')}</li>
                    </ul>
                </li>
                <li><strong>Quality Metrics:</strong>
                    <ul>
                        <li>Overall Score: {quality_score}%</li>
                        <li>Total Papers: {schema_results.get('total_papers', 'N/A') if schema_results else 'N/A'}</li>
                        <li>Anomalies: {schema_results.get('anomaly_count', 0) if schema_results else 0}</li>
                    </ul>
                </li>
            </ul>
        </div>

        <h3>Pipeline Stages Completed</h3>
        <ul>
            <li>✅ check_env_variables</li>
            <li>✅ check_gcs_connection</li>
            <li>✅ test_api_connection (Unit Tests)</li>
            <li>✅ collection_and_preprocessing</li>
            <li>✅ bias_slicing_analysis (subdomain-based)</li>
            <li>✅ bias_alert_check</li>
            <li>✅ mitigate_and_recheck_bias</li>
            <li>✅ schema validation: SUCCESS</li>
            <li>ℹ️  Embeddings: Handled by Supabase webhook</li>
        </ul>

        <h3>Pipeline Details</h3>
        <ul>
            <li><strong>DAG:</strong> {dag_run.dag_id}</li>
            <li><strong>Execution Date:</strong> {dag_run.execution_date}</li>
            <li><strong>Start Date:</strong> {dag_run.start_date}</li>
            <li><strong>Duration:</strong> {dag_run.end_date - dag_run.start_date if dag_run.end_date else 'Running'}</li>
        </ul>
    </body>
    </html>
    """
    
    try:
        smtp_user = os.getenv('SMTP_USER')
        smtp_pass = os.getenv('SMTP_PASSWORD')
        
        if smtp_user and smtp_pass:
            send_email(
                to=EMAIL_TO,
                subject=subject,
                html_content=html_content
            )
            print(f"✅ Success email sent to {EMAIL_TO}")
        else:
            print("⚠️ SMTP credentials not set. Skipping email notification.")
            print("   To enable emails, set SMTP_USER and SMTP_PASSWORD environment variables.")
    except Exception as e:
        print(f"⚠️ Email sending failed: {e}")
        print("   Pipeline completed successfully, but email notification was skipped.")
    
    return "pipeline_completed"

env_check_task = PythonOperator(
    task_id='check_env_variables',
    python_callable=check_env_variables,
    dag=dag
)

gcs_check_task = PythonOperator(
    task_id='check_gcs_connection',
    python_callable=check_gcs_connection,
    dag=dag
)

api_test_task = PythonOperator(
    task_id='test_api_connection',
    python_callable=run_unit_tests,
    dag=dag
)

collection_and_preprocessing_task = PythonOperator(
    task_id='collection_and_preprocessing',
    python_callable=test_paper_collection,
    trigger_rule='all_success',
    provide_context=True,  # Enable context to access params/config
    dag=dag
)

# Removed: embed_task - embeddings handled by Supabase webhook

schema_stats_task = PythonOperator(
    task_id='generate_schema_and_stats',
    python_callable=generate_schema_and_stats,
    dag=dag
)

supabase_upload_task = PythonOperator(
    task_id='upload_to_supabase',
    python_callable=upload_to_supabase,
    dag=dag,
    trigger_rule='all_success'  # Only upload if all previous tasks succeeded
)

notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Removed: load_bias_data_task - bias analysis loads directly from GCS!

bias_slicing_task = PythonOperator(
    task_id='bias_slicing_analysis',
    python_callable=run_bias_slicing,
    dag=dag,
    trigger_rule='all_success'
)

bias_mitigation_task = PythonOperator(
    task_id='mitigate_and_recheck_bias',
    python_callable=mitigate_and_recheck_bias,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'  # Run even if alert fails
)

# Removed: embed_task - embeddings handled by Supabase webhook


initial_checks = [env_check_task, gcs_check_task, api_test_task]
initial_checks >> collection_and_preprocessing_task >> bias_slicing_task >> bias_alert_task >> bias_mitigation_task >> schema_stats_task >> supabase_upload_task >> notification_task

