"""
CiteConnect Controller DAG
Triggers the main test_citeconnect DAG for each enabled domain

This controller:
- Runs weekly (every Sunday at midnight)
- Triggers test_citeconnect DAG for each enabled domain
- Passes domain-specific search terms via config
- Papers per term (100) comes from environment variable
- Sends completion email when all domains are processed
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import os

# ============================================
# LOAD CONFIGURATION
# ============================================
def load_config():
    """Load configuration from YAML file"""
    config_path = '/opt/airflow/configs/collection_config.yaml'
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

# Load config
config = load_config()

# ============================================
# DAG CONFIGURATION
# ============================================
EMAIL_TO = config['notifications']['email_recipients']

default_args = {
    'owner': 'citeconnect-team',
    'depends_on_past': False,
    'start_date': datetime.strptime(config['schedule']['start_date'], '%Y-%m-%d'),
    'email': EMAIL_TO,
    'email_on_failure': config['notifications']['email_on_failure'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# CREATE CONTROLLER DAG
# ============================================
dag = DAG(
    'citeconnect_controller',
    default_args=default_args,
    description='Controller DAG - triggers test_citeconnect weekly for each domain',
    schedule_interval=config['schedule']['interval'],
    catchup=config['schedule']['catchup'],
    tags=['controller', 'weekly', 'citeconnect'],
)

# ============================================
# HELPER FUNCTIONS
# ============================================
def log_domain_start(domain_name, search_terms, **context):
    """Log when a domain processing starts"""
    print(f"🚀 Starting data collection for domain: {domain_name}")
    print(f"📋 Search terms ({len(search_terms)}): {', '.join(search_terms)}")
    print(f"📊 Papers per term: 100 (from environment variable)")
    return f"domain_{domain_name.lower()}_started"

def send_completion_email(**context):
    """Send email when all domains are processed"""
    from airflow.utils.email import send_email
    
    # Get list of processed domains
    domains_processed = [
        domain for domain, cfg in config['taxonomy'].items() 
        if cfg.get('enabled', True)
    ]
    
    domains_html = '\n'.join([f'<li>✅ {domain}</li>' for domain in domains_processed])
    
    try:
        send_email(
            to=EMAIL_TO,
            subject='✅ CiteConnect Weekly Refresh Complete',
            html_content=f'''
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: #2E8B57;">✅ Weekly Data Refresh Completed</h2>
                <p>All enabled domains have been successfully processed:</p>
                <ul>
                    {domains_html}
                </ul>
                <hr>
                <p><strong>Configuration:</strong></p>
                <ul>
                    <li>Papers per search term: 100</li>
                    <li>Domains processed: {len(domains_processed)}</li>
                </ul>
                <p><strong>Next scheduled run:</strong> Next Sunday at midnight</p>
                <p style="color: #666; font-size: 12px;">
                    Check Airflow UI at http://35.226.156.47:8081 for detailed run information.
                </p>
            </body>
            </html>
            '''
        )
        print("✅ Controller completion email sent")
    except Exception as e:
        print(f"⚠️ Failed to send email: {e}")
        print("   Controller completed successfully, but email notification failed")
        print(f"   Processed domains: {', '.join(domains_processed)}")

# ============================================
# CREATE TRIGGER TASKS FOR ENABLED DOMAINS
# ============================================
# Get enabled domains sorted by priority
enabled_domains = [
    (name, cfg) for name, cfg in config['taxonomy'].items() 
    if cfg.get('enabled', True)
]
enabled_domains.sort(key=lambda x: x[1].get('priority', 999))

print(f"📋 Controller DAG: Creating tasks for {len(enabled_domains)} enabled domains")
for domain_name, _ in enabled_domains:
    print(f"   - {domain_name}")

previous_task = None

for domain_name, domain_config in enabled_domains:
    search_terms = domain_config['search_terms']
    search_terms_str = ', '.join(search_terms)
    
    # Log task (for visibility in Airflow UI)
    log_task = PythonOperator(
        task_id=f'log_start_{domain_name.lower()}',
        python_callable=log_domain_start,
        op_kwargs={
            'domain_name': domain_name,
            'search_terms': search_terms
        },
        provide_context=True,
        dag=dag
    )
    
    # Trigger task - triggers your EXISTING test_citeconnect DAG
    trigger_task = TriggerDagRunOperator(
        task_id=f'trigger_{domain_name.lower()}',
        trigger_dag_id='test_citeconnect',
        conf={
            'SEARCH_TERMS': search_terms_str,
            'COLLECTION_DOMAIN': domain_name,
            'COLLECTION_SUBDOMAINS': ''
        },
        wait_for_completion=True,
        poke_interval=60,
        dag=dag
    )
    
    # Set dependencies
    log_task >> trigger_task
    
    # Chain domains sequentially (run one after another)
    if previous_task:
        previous_task >> log_task
    
    previous_task = trigger_task

# ============================================
# FINAL NOTIFICATION
# ============================================
notification_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_email,
    provide_context=True,
    dag=dag
)

# Connect final notification
if previous_task:
    previous_task >> notification_task
else:
    # If no domains enabled, still create the task (but it will run alone)
    notification_task

print("✅ Controller DAG created successfully")