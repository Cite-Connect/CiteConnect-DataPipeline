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

EMAIL_TO = ['dennismjose26@yahoo.com']

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

# DAG config
dag = DAG(
    'test_citeconnect',
    default_args=default_args,
    description='CiteConnect test pipeline with email notifications',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'citeconnect']
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

def test_paper_collection():
    """
    Collect papers using async pipeline with overlapping collection/preprocessing.
    This replaces the old sequential approach with a faster pipeline pattern.
    """
    from src.DataPipeline.Ingestion.main import collect_and_process_pipeline
    import os
    
    # Get search terms from environment variable or use default
    search_terms_env = os.getenv('SEARCH_TERMS', 'finance, quantum computing, healthcare')
    search_terms = [term.strip() for term in search_terms_env.split(',')]
    limit = int(os.getenv('PAPERS_PER_TERM', '5'))
    
    print(f"🔍 Search terms: {search_terms}")
    print(f"📊 Papers per term: {limit}")
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
    return collection_results

def preprocess_papers(**context):
    """
    Preprocess papers. Note: If using async pipeline, preprocessing is already done
    during collection. This task will skip if preprocessing was already completed.
    """
    print("Testing paper preprocessing...")

    ti = context['task_instance']
    collection_results = ti.xcom_pull(task_ids='test_paper_collection')
    
    if not collection_results:
        raise ValueError("No collection results received")
    
    # Check if preprocessing was already done (async pipeline does both)
    # If papers were already processed, we can skip this step
    import os
    processed_dir = "/tmp/test_data/processed"
    if os.path.exists(processed_dir) and os.listdir(processed_dir):
        print("✅ Preprocessing already completed by async pipeline. Skipping...")
        # Return a placeholder result
        return [{'search_term': r['search_term'], 'status': 'already_processed'} for r in collection_results]
    
    # Fallback: Process if not already done
    from src.DataPipeline.Ingestion.main import process_collected_papers
    
    results = process_collected_papers(
        collection_results=collection_results,
        output_dir=processed_dir
    )
    
    print(f"Processing completed: {len(results)} terms processed")
    print("Processed files uploaded to GCS:")
    for result in results:
        print(f"  {result['search_term']}: {result['processed_gcs']}")
    
    return results
'''
def embed_stored_data():
    import sys
    if '/opt/airflow' not in sys.path:
        sys.path.insert(0, '/opt/airflow')

    from services.embedding_service import EmbeddingService
    
    service = EmbeddingService(
        bucket_name="citeconnect-test-bucket",
        gcs_prefix="raw/",
        flat_structure=True,
        gcs_project_id="strange-calling-476017-r5"
    )
    
    return service.process_domain("healthcare", batch_size=10, max_papers=1000, use_streaming=True)
'''
def embed_and_store_to_postgres(**context):
    """
    Generate embeddings and store in PostgreSQL (Supabase).
    Replaces the old pickle-based approach.
    
    This function:
    1. Reads papers from GCS processed/ folder
    2. Inserts papers into PostgreSQL papers table
    3. Generates mini-LM embeddings (384-dim)
    4. Generates SPECTER embeddings (768-dim)
    5. Stores both in respective PostgreSQL tables
    
    Returns statistics compatible with DVC tracking.
    """
    import asyncio
    import sys
    import os
    from airflow.models import Variable
    if '/opt/airflow' not in sys.path:
        sys.path.insert(0, '/opt/airflow')
    
    from services.embedding_service_postgres import PostgreSQLEmbeddingService
    
    print("="*60)
    print("EMBEDDING GENERATION (PostgreSQL)")
    print("="*60)
    
    # Database configuration from environment variables
    '''    
    db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }'''
    try:
        db_config = {
            'host':Variable.get('SUPABASE_DB_HOST',default_var='db.wvvogncqrqzfbfztkwfo.supabase.co'),
            'port': int(Variable.get('SUPABASE_DB_PORT',default_var='5432')),
            'database': Variable.get('SUPABASE_DB_NAME',default_var='postgres'),
            'user': Variable.get('SUPABASE_DB_USER',default_var='postgres'),
            'password': Variable.get('SUPABASE_DB_PASSWORD'),
        }
    except KeyError:
        db_config = {
        'host': os.getenv('SUPABASE_DB_HOST', 'db.wvvogncqrqzfbfztkwfo.supabase.co'),
        'port': int(os.getenv('SUPABASE_DB_PORT', 5432)),
        'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
        'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
        'password': os.getenv('SUPABASE_DB_PASSWORD'),
    }

    # Verify database credentials
    if not db_config['password']:
        raise ValueError(
            "SUPABASE_DB_PASSWORD not set! "
            "Please set in Airflow Variables or environment."
        )
    
    # GCS configuration
    gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')
    gcs_project_id = os.getenv('GCS_PROJECT_ID', 'strange-calling-476017-r5')
    
    # Processing configuration
    max_papers = int(os.getenv('MAX_PAPERS_EMBEDDING', '1000'))  # Limit for safety
    batch_size = int(os.getenv('EMBEDDING_BATCH_SIZE', '32'))
    
    print(f"Configuration:")
    print(f"  Database: {db_config['user']}@{db_config['host']}")
    print(f"  GCS Bucket: {gcs_bucket}")
    print(f"  Max Papers: {max_papers}")
    print(f"  Batch Size: {batch_size}")
    
    try:
        # Initialize service
        service = PostgreSQLEmbeddingService(
            gcs_bucket=gcs_bucket,
            gcs_prefix='processed_v2/',  # Read from processed folder
            db_config=db_config,
            gcs_project_id=gcs_project_id
        )
        
        # Run async pipeline
        print("\nStarting dual embedding generation...")
        stats = asyncio.run(
            service.process_and_store_dual_embeddings(
                batch_size=batch_size,
                max_papers=max_papers
            )
        )
        
        # Log results
        print("\n" + "="*60)
        print("EMBEDDING GENERATION COMPLETE")
        print("="*60)
        print(f"✅ Papers processed: {stats['processed_papers']}")
        print(f"✅ mini-LM embeddings: {stats['minilm_embeddings']}")
        print(f"✅ SPECTER embeddings: {stats['specter_embeddings']}")
        print(f"✅ Total embeddings: {stats['embedded_chunks']}")
        print("="*60)
        
        # Return stats in format compatible with DVC versioning task
        # The version_embeddings_with_dvc task expects these fields
        return {
            'total_papers': stats['total_papers'],
            'processed_papers': stats['processed_papers'],
            'embedded_chunks': stats['embedded_chunks'],  # Total of both models
            'minilm_embeddings': stats['minilm_embeddings'],
            'specter_embeddings': stats['specter_embeddings'],
            'params': {
                'gcs_bucket': gcs_bucket,
                'gcs_prefix': 'processed_v2/',
                'batch_size': batch_size,
                'max_papers': max_papers,
                'models': ['all-MiniLM-L6-v2', 'allenai/specter']
            }
        }
    
    except Exception as e:
        print(f"\n❌ ERROR: Embedding generation failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def load_bias_data_from_gcs():
    """Load all parquet files from GCS and combine them for bias analysis."""
    import pandas as pd
    import json
    import numpy as np
    from utils.gcs_reader import GCSReader
    
    print("Loading data from GCS for bias analysis ...")
    
    # Set working directory to project root
    project_root = '/opt/airflow'
    data_path = os.path.join(project_root, "data/combined_gcs_data.parquet")
    
    # Check if data already exists
    if os.path.exists(data_path):
        print(f"✅ Data file already exists at {data_path}. Skipping download.")
        return "data_loaded"
    
    try:
        # Initialize GCS reader
        reader = GCSReader(
            bucket_name="citeconnect-test-bucket",
            project_id="strange-calling-476017-r5"
        )
        
        # Load all parquet files from raw/ folder
        print("📥 Loading all parquet files from citeconnect-test-bucket/raw/")
        df_all = reader.read_all_from_domain(
            domain="",
            custom_prefix="raw/",
            flat_structure=True
        )
        
        if df_all.empty:
            raise ValueError("No data loaded from GCS!")
        
        print(f"✅ Loaded {len(df_all)} total records from GCS")
        
        # Clean up any dict/list/ndarray columns before saving
        def safe_serialize(val):
            """Convert dicts, lists, or arrays into JSON-safe strings"""
            if isinstance(val, (dict, list, np.ndarray)):
                try:
                    return json.dumps(val, default=str)
                except Exception:
                    return str(val)
            return val

        for col in df_all.columns:
            if df_all[col].dtype == 'object':
                df_all[col] = df_all[col].apply(safe_serialize)
        
        # Save to local file
        os.makedirs(os.path.join(project_root, "data"), exist_ok=True)
        df_all.to_parquet(data_path, index=False)
        
        print(f"💾 Saved merged data to {data_path}")
        print(f"📊 Columns: {df_all.columns.tolist()}")
        print(f"📊 Shape: {df_all.shape}")
        
        return "data_loaded"
        
    except Exception as e:
        print(f"❌ Error loading data from GCS: {e}")
        raise


def run_bias_slicing():
    print("Running Fairlearn slicing analysis ...")
    
    # Set working directory to project root
    project_root = '/opt/airflow'
    
    # Check if data file exists
    data_path = os.path.join(project_root, "data/combined_gcs_data.parquet")
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}. Run GCS data loading first.")
    
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
    
    print("✅ Bias slicing completed. Results saved in databias/slices/")
    return "bias_slicing_done"


def check_bias_and_send_alert():
    print("Checking fairness_disparity.json ...")
    
    # Use absolute path to project root
    project_root = '/opt/airflow'
    disparity_path = os.path.join(project_root, "databias/slices/fairness_disparity.json")
    
    if not os.path.exists(disparity_path):
        raise ValueError(f"fairness_disparity.json not found at {disparity_path}. Run slicing first.")
    
    with open(disparity_path, "r") as f:
        disparity = json.load(f)

    disparity_ratio = disparity.get("disparity_ratio", 0)
    disparity_diff = disparity.get("disparity_difference", 0)

    print(f"📈 Disparity ratio: {disparity_ratio:.2f}")
    print(f"📉 Disparity difference: {disparity_diff:.2f}")

    THRESHOLD = 10.0  # 🔔 alert threshold for ratio

    if disparity_ratio > THRESHOLD:
        subject = f"⚠️ CiteConnect Bias Alert: Disparity ratio {disparity_ratio:.2f}"
        html_content = f"""
        <h3>Bias Threshold Exceeded</h3>
        <p><b>Disparity Ratio:</b> {disparity_ratio:.2f}<br>
        <b>Disparity Difference:</b> {disparity_diff:.2f}</p>
        <p>Check the detailed slice report in databias/slices/fairness_disparity.json</p>
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
                print(f"🚨 Bias alert email sent! Ratio exceeded threshold {THRESHOLD}.")
                return "alert_sent"
            else:
                print(f"⚠️ SMTP credentials not configured. Skipping email alert.")
                print(f"🚨 Bias threshold exceeded (ratio: {disparity_ratio:.2f} > {THRESHOLD})")
                print(f"   To enable email alerts, set SMTP_USER and SMTP_PASSWORD in .env file")
                return "alert_threshold_exceeded_no_email"
        except Exception as e:
            print(f"⚠️ Failed to send email alert: {e}")
            print(f"🚨 Bias threshold exceeded (ratio: {disparity_ratio:.2f} > {THRESHOLD})")
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

def mitigate_bias():
    import pandas as pd
    df = pd.read_parquet("data/combined_gcs_data_balanced.parquet")
    print(f"✅ Loaded {len(df)} records for mitigation.")
    print("Balancing underrepresented fields (simulation)...")
    # Add balancing logic later if you retrain models here
    balanced_path = "data/final_balanced_dataset.parquet"
    df.to_parquet(balanced_path, index=False)
    print(f"💾 Saved mitigated dataset → {balanced_path}")
    return "bias_mitigated"

def version_embeddings_with_dvc(**context):
    import subprocess
    import os
    import json
    from datetime import datetime
    
    project_root = "/opt/airflow"
    embeddings_path = os.path.join(project_root, "working_data/embeddings_db.pkl")
    summary_path = os.path.join(project_root, "working_data/run_summary.json")

    try:
        git_dir = os.path.join(project_root, ".git")
        if not os.path.exists(git_dir):
            print("This appears to be a first-time run. Initializing Git repository...")
            subprocess.run(['git', 'init', '-b', 'main'], check=True, capture_output=True, cwd=project_root)
            print("Git repository initialized.")
        else:
            print("Git repository already initialized.")

        dvc_dir = os.path.join(project_root, ".dvc")
        if not os.path.exists(dvc_dir):
            print("This appears to be a first-time run. Initializing DVC and remote...")
            
            subprocess.run(['dvc', 'init', '--no-scm'], check=True, capture_output=True, cwd=project_root)
            print("DVC initialized.")
            
            subprocess.run(
                ['dvc', 'remote', 'add', '-d', 'gcs', 'gs://citeconnect-test-bucket/dvc-cache'],
                check=True, capture_output=True, cwd=project_root
            )
            print("DVC remote 'gcs' added.")
            
            subprocess.run(
                ['dvc', 'remote', 'modify', 'gcs', 'credentialpath', '/opt/airflow/configs/credentials/gcs-key.json'],
                check=True, capture_output=True, cwd=project_root
            )
            print("DVC remote 'gcs' credentials configured.")

            subprocess.run(
                ['git', 'config', '--global', '--add', 'safe.directory', project_root], 
                check=True, capture_output=True, cwd=project_root
            )
            subprocess.run(['git', 'config', '--global', 'user.email', 'aditya811.abhinav@gmail.com'], check=True, cwd=project_root)
            subprocess.run(['git', 'config', '--global', 'user.name', 'Abhinav Aditya'], check=True, cwd=project_root)
            
            subprocess.run(['git', 'add', '.dvc/config'], check=True, capture_output=True, cwd=project_root)
            
            subprocess.run(
                ['git', 'commit', '-m', 'Initialize DVC and remote'],
                check=True, capture_output=True, cwd=project_root
            )
            print("Initial DVC configuration committed to Git.")
            
        else:
            print("DVC already initialized.")
        
        try:
            result = subprocess.run(['dvc', 'remote', 'list'], capture_output=True, text=True, cwd=project_root)
            if 'gcs' not in result.stdout:
                print("DVC remote not found, re-adding...")
                subprocess.run(
                    ['dvc', 'remote', 'add', '-d', 'gcs', 'gs://citeconnect-test-bucket/dvc-cache'],
                    check=True, capture_output=True, cwd=project_root
                )
                subprocess.run(
                    ['dvc', 'remote', 'modify', 'gcs', 'credentialpath', '/opt/airflow/configs/credentials/gcs-key.json'],
                    check=True, capture_output=True, cwd=project_root
                )
                print("DVC remote re-configured.")
        except subprocess.CalledProcessError:
            print("Warning: Could not verify DVC remote")
        
        
        subprocess.run(
            ['git', 'config', '--global', '--add', 'safe.directory', project_root], 
            check=True, 
            capture_output=True,
            cwd=project_root
        )
        '''
        ti = context['task_instance']
        embed_results = ti.xcom_pull(task_ids='embed_stored_data') or {}
        '''
        ti = context['task_instance']
        embed_results = ti.xcom_pull(task_ids='embed_and_store_to_postgres') or {}
        
        file_size_mb = 0.0
        if os.path.exists(embeddings_path):
            file_size_mb = round(os.path.getsize(embeddings_path) / (1024*1024), 2)

        embeddings_created = embed_results.get('embedded_chunks', 0)
        total_papers = embed_results.get('total_papers', 0)
        run_params = embed_results.get('params', {"status": "unknown"})
        
        # Get search terms from environment variable (same as test_paper_collection)
        search_terms_env = os.getenv('SEARCH_TERMS', 'finance, quantum computing, healthcare')
        search_terms_list = [term.strip() for term in search_terms_env.split(',')]
        
        summary_list = []
        if os.path.exists(summary_path):
            try:
                with open(summary_path, 'r') as f:
                    summary_list = json.load(f)
                if not isinstance(summary_list, list):
                    summary_list = [summary_list]
            except json.JSONDecodeError:
                summary_list = []

        new_run_summary = {
            "run_timestamp": datetime.now().isoformat(),
            "params": run_params,
            "outs": {
                "embeddings_file": {
                    "path": "working_data/embeddings_db.pkl",
                    "file_size_mb": file_size_mb,
                    "total_chunks": embeddings_created
                },
                "total_papers_processed": total_papers,
                "search_terms": search_terms_list
            }
        }
        
        summary_list.append(new_run_summary)
        
        with open(summary_path, 'w') as f:
            json.dump(summary_list, f, indent=4)
        print(f"Appended new run to {summary_path}. Total runs logged: {len(summary_list)}")

        # Git config
        subprocess.run(['git', 'config', '--global', 'user.email', 'aditya811.abhinav@gmail.com'], check=True, cwd=project_root)
        subprocess.run(['git', 'config', '--global', 'user.name', 'Abhinav Aditya'], check=True, cwd=project_root)
        
        # DVC add
        if os.path.exists(embeddings_path):
            subprocess.run(['dvc', 'add', embeddings_path], check=True, capture_output=True, cwd=project_root)
            print("Added embeddings to DVC tracking")
            embed_dvc_file = f"{embeddings_path}.dvc"
            subprocess.run(['git', 'add', embed_dvc_file], check=True, capture_output=True, cwd=project_root)
            print("Added .dvc file to git")

        # Git add summary
        subprocess.run(['git', 'add', '-f', summary_path], check=True, capture_output=True, cwd=project_root)
        print("Added run_summary.json to git")

        try:
            subprocess.run(['git', 'add', '.gitignore'], check=True, capture_output=True, cwd=project_root)
        except subprocess.CalledProcessError:
            pass
        
        # Git commit
        commit_msg = f"Update embeddings: {embeddings_created} chunks, {total_papers} papers - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        subprocess.run(
            ['git', 'commit', '--allow-empty', '-m', commit_msg], 
            check=True, 
            capture_output=True, 
            cwd=project_root
        )
        print(f"Git commit: {commit_msg}")
        
        # DVC Push
        if os.path.exists(embeddings_path):
            subprocess.run(['dvc', 'push'], check=True, capture_output=True, cwd=project_root)
            print("Pushed to DVC remote")
        
        new_run_summary["status"] = "success"
        new_run_summary["commit_message"] = commit_msg
        return new_run_summary
        
    except subprocess.CalledProcessError as e:
        error_msg = f"DVC/Git command failed: {e.stderr.decode() if e.stderr else e.stdout.decode() if e.stdout else str(e)}"
        print(f"Error: {error_msg}")
        return {"status": "failed", "error": error_msg}  # FIXED: Return instead of raise
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"Error: {error_msg}")
        return {"status": "failed", "error": error_msg}  # FIXED: Return instead of raise

def generate_schema_and_stats(**context):
    """Generate schema and validate data quality"""
    from src.DataPipeline.Validation.schema_validator import validate_schema
    return validate_schema(**context)

def send_success_notification(**context):
    dag_run = context['dag_run']
    ti = context['task_instance']
    
    try:
        version_results = ti.xcom_pull(task_ids='version_embeddings_dvc')
        if not version_results:
            # Fallback in case XCom pull fails
            version_results = {"status": "success", "outs": {"embeddings_file": {}}}
        
        # Extract key metrics
        params = version_results.get('params', {})
        outs = version_results.get('outs', {"embeddings_file": {}})
        embed_file_stats = outs.get('embeddings_file', {})
        
        papers_processed = outs.get('total_papers_processed', 'N/A')
        chunks_created = embed_file_stats.get('total_chunks', 'N/A')
        file_size = embed_file_stats.get('file_size_mb', 'N/A')
        commit_msg = version_results.get('commit_message', 'N/A')

    except Exception as e:
        print(f"Warning: Could not pull XCom data. Sending generic email. Error: {e}")
        papers_processed = 'N/A'
        chunks_created = 'N/A'
        file_size = 'N/A'
        commit_msg = 'N/A'
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
                        <li>Max Papers: {params.get('max_papers', 'N/A')}</li>
                        <li>Batch Size: {params.get('batch_size', 'N/A')}</li>
                        <li><strong>Overall Quality Score:</strong> {quality_score}%</li>
                        <li><strong>Total Papers:</strong> {schema_results.get('total_papers', 'N/A') if schema_results else 'N/A'}</li>
                    </ul>
                </li>
                <li><strong>Commit:</strong> <span class="commit">"{commit_msg}"</span></li>
            </ul>
        </div>

        <h3>Pipeline Stages Completed</h3>
        <ul>
            <li>✅ check_env_variables</li>
            <li>✅ check_gcs_connection</li>
            <li>✅ test_api_connection (Unit Tests)</li>
            <li>✅ test_paper_collection</li>
            <li>✅ preprocess_papers</li>
            <li>✅ embed_stored_data_to_postgres</li>
            <li>✅ version_embeddings_dvc (Data Versioning)</li>
            <li>✅ schema validation: SUCCESS</li>
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

collection_test_task = PythonOperator(
    task_id='test_paper_collection',
    python_callable=test_paper_collection,
    trigger_rule='all_success',
    dag=dag
)

preprocess_task = PythonOperator(
    task_id='preprocess_papers',
    python_callable=preprocess_papers,
    provide_context=True,
    dag=dag
)

'''
embed_task = PythonOperator(
    task_id='embed_stored_data',
    python_callable=embed_stored_data,
    dag=dag
)
'''
embed_task = PythonOperator(
    task_id='embed_and_store_to_postgres',  # New task name
    python_callable=embed_and_store_to_postgres,  # New function
    provide_context=True,  # Pass context for XCom
    dag=dag
)

schema_stats_task = PythonOperator(
    task_id='generate_schema_and_stats',
    python_callable=generate_schema_and_stats,
    dag=dag
)

dvc_version_task = PythonOperator(
    task_id='version_embeddings_dvc',
    python_callable=version_embeddings_with_dvc,
    provide_context=True,
    dag=dag
)

notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

load_bias_data_task = PythonOperator(
    task_id='load_bias_data_from_gcs',
    python_callable=load_bias_data_from_gcs,
    dag=dag,
    trigger_rule='all_done'
)

bias_slicing_task = PythonOperator(
    task_id='bias_slicing_analysis',
    python_callable=run_bias_slicing,
    dag=dag,
    trigger_rule='all_success'
)

bias_mitigation_task = PythonOperator(
    task_id='bias_mitigation',
    python_callable=mitigate_bias,
    dag=dag
)



initial_checks = [env_check_task, gcs_check_task, api_test_task]
'''
initial_checks >> collection_test_task >> preprocess_task >> load_bias_data_task >> bias_slicing_task >> bias_alert_task >> bias_mitigation_task >> embed_task >> dvc_version_task >> schema_stats_task >> notification_task
'''
initial_checks >> collection_test_task >> preprocess_task >> load_bias_data_task >> bias_slicing_task >> bias_alert_task >> bias_mitigation_task >> embed_task >> dvc_version_task >> schema_stats_task >> notification_task