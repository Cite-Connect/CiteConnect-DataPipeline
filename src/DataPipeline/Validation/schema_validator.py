"""
Schema Validation Module for CiteConnect
Validates schema and data quality with alerting on quality drops
Enhanced with anomaly detection for production monitoring
"""

import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Lightweight anomaly detection for CiteConnect data quality monitoring"""
    
    def __init__(self, history_window: int = 10):
        """
        Args:
            history_window: Number of previous runs to use for baseline
        """
        self.history_window = history_window
    
    def detect_anomalies(self, df: pd.DataFrame, historical_schemas: List[Dict]) -> Dict:
        """
        Detect anomalies using pattern analysis and domain-specific checks
        
        Returns:
            Dictionary with anomaly findings and severity levels
        """
        anomalies = {
            'timestamp': datetime.now().isoformat(),
            'has_anomalies': False,
            'severity': 'normal',  # normal, warning, critical
            'detected_anomalies': []
        }
        
        # Method 1: Pattern Anomalies (Volume & Missing Data)
        pattern_anomalies = self._detect_pattern_anomalies(df, historical_schemas)
        if pattern_anomalies:
            anomalies['detected_anomalies'].extend(pattern_anomalies)
        
        # Method 2: Domain-Specific Anomalies (CiteConnect-specific)
        domain_anomalies = self._detect_domain_anomalies(df)
        if domain_anomalies:
            anomalies['detected_anomalies'].extend(domain_anomalies)
        
        # Set overall status
        if anomalies['detected_anomalies']:
            anomalies['has_anomalies'] = True
            critical_count = sum(1 for a in anomalies['detected_anomalies'] if a['severity'] == 'critical')
            warning_count = sum(1 for a in anomalies['detected_anomalies'] if a['severity'] == 'warning')
            
            if critical_count > 0:
                anomalies['severity'] = 'critical'
            elif warning_count > 2:
                anomalies['severity'] = 'critical'
            elif warning_count > 0:
                anomalies['severity'] = 'warning'
        
        return anomalies
    
    def _detect_pattern_anomalies(self, df: pd.DataFrame, history: List[Dict]) -> List[Dict]:
        """
        Detect sudden changes in data patterns:
        - Volume spikes/drops
        - Missing data rate increases
        """
        anomalies = []
        
        if len(history) < 3:
            logger.info("⚠️ Not enough historical data for pattern anomaly detection (need 3+ runs)")
            return anomalies
        
        # Check 1: Volume Changes (Sudden spike or drop in paper count)
        current_count = len(df)
        historical_counts = [h.get('total_papers', 0) for h in history[-self.history_window:]]
        
        if historical_counts and len(historical_counts) >= 3:
            hist_mean = np.mean(historical_counts)
            hist_std = np.std(historical_counts)
            
            if hist_std > 0 and hist_mean > 0:
                z_score = abs(current_count - hist_mean) / hist_std
                percent_change = abs(current_count - hist_mean) / hist_mean * 100
                
                if z_score > 2.5 or percent_change > 50:  # Significant change
                    anomalies.append({
                        'type': 'volume_anomaly',
                        'column': 'total_papers',
                        'severity': 'critical' if percent_change > 75 else 'warning',
                        'description': f'Paper count changed by {percent_change:.1f}% from historical average',
                        'details': {
                            'current_count': current_count,
                            'historical_avg': int(hist_mean),
                            'expected_range': f'{int(hist_mean - 2*hist_std)} - {int(hist_mean + 2*hist_std)}',
                            'change_percentage': round(percent_change, 1)
                        }
                    })
        
        # Check 2: Missing Data Spike
        current_missing_rate = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        
        historical_missing = []
        for h in history[-self.history_window:]:
            if h.get('missing_count') and h.get('total_papers') and h.get('columns'):
                total_missing = sum(h['missing_count'].values())
                total_cells = h['total_papers'] * len(h['columns'])
                if total_cells > 0:
                    historical_missing.append((total_missing / total_cells) * 100)
        
        if historical_missing and len(historical_missing) >= 3:
            hist_mean_missing = np.mean(historical_missing)
            
            if current_missing_rate > hist_mean_missing * 1.5:  # 50% increase in missing data
                anomalies.append({
                    'type': 'missing_data_spike',
                    'column': 'all_columns',
                    'severity': 'critical' if current_missing_rate > hist_mean_missing * 2 else 'warning',
                    'description': 'Missing data rate increased significantly',
                    'details': {
                        'current_rate': round(current_missing_rate, 2),
                        'historical_avg': round(hist_mean_missing, 2),
                        'increase_factor': round(current_missing_rate / hist_mean_missing, 2)
                    }
                })
        
        return anomalies
    
    def _detect_domain_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """
        CiteConnect-specific anomaly detection:
        - Content extraction failures
        - Duplicate papers
        - Citation anomalies
        - Temporal bias
        """
        anomalies = []
        
        # Check 1: Content Extraction Failure Spike
        if 'has_intro' in df.columns:
            missing_intro = (~df['has_intro']).sum()
            missing_pct = (missing_intro / len(df)) * 100
            
            if missing_pct > 60:  # >60% failed extraction is critical
                anomalies.append({
                    'type': 'extraction_failure_spike',
                    'column': 'has_intro',
                    'severity': 'critical' if missing_pct > 80 else 'warning',
                    'description': f'High content extraction failure rate ({missing_pct:.1f}%)',
                    'details': {
                        'failed_count': missing_intro,
                        'failure_percentage': round(missing_pct, 2),
                        'successful_count': len(df) - missing_intro
                    }
                })
        
        # Check 2: Duplicate Papers (Deduplication Failure)
        if 'paperId' in df.columns:
            duplicate_count = df['paperId'].duplicated().sum()
            duplicate_pct = (duplicate_count / len(df)) * 100
            
            if duplicate_count > 0:  # Should be 0 after deduplication!
                anomalies.append({
                    'type': 'duplicate_papers',
                    'column': 'paperId',
                    'severity': 'critical' if duplicate_pct > 5 else 'warning',
                    'description': f'Duplicate papers detected ({duplicate_count} duplicates)',
                    'details': {
                        'duplicate_count': duplicate_count,
                        'duplicate_percentage': round(duplicate_pct, 2),
                        'unique_papers': df['paperId'].nunique()
                    }
                })
        
        # Check 3: Suspiciously Low Citation Counts
        if 'citationCount' in df.columns:
            zero_citations = (df['citationCount'] == 0).sum()
            zero_pct = (zero_citations / len(df)) * 100
            
            if zero_pct > 70:  # >70% papers with 0 citations is unusual
                anomalies.append({
                    'type': 'low_citation_anomaly',
                    'column': 'citationCount',
                    'severity': 'warning',
                    'description': f'Unusually high percentage of zero-citation papers ({zero_pct:.1f}%)',
                    'details': {
                        'zero_citation_count': zero_citations,
                        'zero_citation_percentage': round(zero_pct, 2),
                        'avg_citations': round(df['citationCount'].mean(), 2)
                    }
                })
        
        # Check 4: Temporal Bias (All papers from same year)
        if 'year' in df.columns:
            year_counts = df['year'].value_counts()
            if len(year_counts) > 0:
                top_year_pct = (year_counts.iloc[0] / len(df)) * 100
                
                if top_year_pct > 70 and len(year_counts) > 1:
                    anomalies.append({
                        'type': 'temporal_bias',
                        'column': 'year',
                        'severity': 'warning',
                        'description': f'Papers heavily concentrated in year {year_counts.index[0]} ({top_year_pct:.1f}%)',
                        'details': {
                            'dominant_year': int(year_counts.index[0]),
                            'year_percentage': round(top_year_pct, 2),
                            'year_diversity': len(year_counts)
                        }
                    })
        
        # Check 5: Domain Distribution Skew
        if 'domain' in df.columns:
            domain_counts = df['domain'].value_counts()
            if len(domain_counts) > 1:
                top_domain_pct = (domain_counts.iloc[0] / len(df)) * 100
                
                if top_domain_pct > 85:  # >85% from one domain
                    anomalies.append({
                        'type': 'domain_skew',
                        'column': 'domain',
                        'severity': 'warning',
                        'description': f'Papers heavily skewed towards {domain_counts.index[0]} domain ({top_domain_pct:.1f}%)',
                        'details': {
                            'dominant_domain': str(domain_counts.index[0]),
                            'domain_percentage': round(top_domain_pct, 2),
                            'total_domains': len(domain_counts)
                        }
                    })
        
        return anomalies


class SchemaValidator:
    """Validates data schema and tracks quality over time"""
    
    def __init__(self, schema_dir: str = "data/schemas"):
        self.schema_dir = Path(schema_dir)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.anomaly_detector = AnomalyDetector(history_window=10)
        
    def generate_and_validate(self, df: pd.DataFrame) -> Dict:
        """Generate schema and validate quality"""
        
        # Generate schema
        schema = {
            'timestamp': datetime.now().isoformat(),
            'total_papers': len(df),
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'missing_count': df.isnull().sum().to_dict(),
            'numeric_stats': {},
            'quality_metrics': {}
        }
        
        # Numeric statistics
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            schema['numeric_stats'][col] = {
                'mean': float(df[col].mean()) if df[col].notna().any() else None,
                'min': float(df[col].min()) if df[col].notna().any() else None,
                'max': float(df[col].max()) if df[col].notna().any() else None,
                'missing_count': int(df[col].isnull().sum())
            }
        
        # Quality metrics
        completeness = self._calculate_completeness(df)
        validity = self._calculate_validity(df)
        
        schema['quality_metrics'] = {
            'completeness': completeness,
            'validity': validity,
            'overall_score': (completeness + validity) / 2
        }
        
        return schema
    
    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        """Calculate data completeness (0-100)"""
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        if total_cells == 0:
            return 100.0
        return round(((total_cells - missing_cells) / total_cells) * 100, 2)
    
    def _calculate_validity(self, df: pd.DataFrame) -> float:
        """Calculate data validity"""
        issues = 0
        
        # Check year range
        if 'year' in df.columns:
            invalid_years = df[(df['year'] < 1950) | (df['year'] > 2025)].shape[0]
            issues += invalid_years
        
        # Check negative citations
        if 'citationCount' in df.columns:
            invalid_cites = df[df['citationCount'] < 0].shape[0]
            issues += invalid_cites
        
        total_rows = len(df)
        if total_rows == 0:
            return 0.0
        return round(((total_rows * 2 - issues) / (total_rows * 2)) * 100, 2)
    
    def compare_with_previous(self, current_schema: Dict) -> Dict:
        """Compare with previous schema and detect quality drops"""
        
        schema_files = sorted(self.schema_dir.glob("schema_*.json"))
        if len(schema_files) <= 1:
            return {'has_previous': False}
        
        # Load previous schema
        previous_file = schema_files[-2]
        with open(previous_file, 'r') as f:
            previous_schema = json.load(f)
        
        current_quality = current_schema.get('quality_metrics', {})
        previous_quality = previous_schema.get('quality_metrics', {})
        
        alert = {
            'has_previous': True,
            'quality_dropped': False,
            'drop_details': {}
        }
        
        if previous_quality:
            for metric in ['completeness', 'validity', 'overall_score']:
                current_val = current_quality.get(metric, 0)
                previous_val = previous_quality.get(metric, 0)
                
                if current_val < previous_val - 5:  # 5% drop threshold
                    alert['quality_dropped'] = True
                    alert['drop_details'][metric] = {
                        'current': current_val,
                        'previous': previous_val,
                        'drop': round(previous_val - current_val, 2)
                    }
        
        return alert
    
    def save_schema(self, schema: Dict) -> str:
        """Save schema to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.schema_dir / f"schema_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(schema, f, indent=2, default=str)
        
        return str(filename)
    
    def validate_with_anomaly_detection(self, df: pd.DataFrame) -> Dict:
        """
        Complete validation with anomaly detection
        
        Returns:
            Full validation report including anomalies
        """
        # Standard validation
        schema = self.generate_and_validate(df)
        
        # Load historical schemas for anomaly detection
        historical_schemas = self._load_historical_schemas()
        
        # Anomaly detection
        anomalies = self.anomaly_detector.detect_anomalies(df, historical_schemas)
        
        # Add anomalies to schema
        schema['anomalies'] = anomalies
        
        return schema
    
    def _load_historical_schemas(self) -> List[Dict]:
        """Load previous schemas for comparison"""
        schemas = []
        schema_files = sorted(self.schema_dir.glob("schema_*.json"))
        
        for schema_file in schema_files[-10:]:  # Last 10 runs
            try:
                with open(schema_file, 'r') as f:
                    schemas.append(json.load(f))
            except Exception as e:
                logger.warning(f"Could not load schema file {schema_file}: {e}")
                continue
        
        return schemas


def send_anomaly_alert(anomalies: Dict, schema: Dict):
    """Send email alert for detected anomalies with bias visualization"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.image import MIMEImage
    
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')
    alert_email = os.getenv('ALERT_EMAIL', smtp_user)
    
    if not smtp_user or not smtp_password:
        logger.warning("⚠️ SMTP credentials not configured, skipping email alert")
        return
    
    # Try to download bias visualization from GCS
    bias_viz_data = None
    try:
        from google.cloud import storage
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        if bucket_name:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            
            # Download bias_metrics.png
            blob = bucket.blob('databias_v2/visualizations/bias_metrics.png')
            if blob.exists():
                bias_viz_data = blob.download_as_bytes()
                logger.info("✅ Downloaded bias visualization from GCS")
            else:
                logger.info("ℹ️ No bias visualization found in GCS")
    except Exception as e:
        logger.warning(f"⚠️ Could not download bias visualization: {e}")
    
    # Build alert message
    severity_emoji = "🔴" if anomalies['severity'] == 'critical' else "⚠️"
    subject = f"{severity_emoji} CiteConnect Data Anomaly Alert - {anomalies['severity'].upper()}"
    
    # Build HTML body
    anomaly_details = ""
    for anomaly in anomalies['detected_anomalies']:
        sev_icon = "🔴" if anomaly['severity'] == 'critical' else "⚠️"
        anomaly_details += f"""
        <div style="margin: 10px 0; padding: 10px; background-color: {'#ffebee' if anomaly['severity'] == 'critical' else '#fff3e0'}; border-left: 4px solid {'#f44336' if anomaly['severity'] == 'critical' else '#ff9800'};">
            <h4 style="margin: 0 0 5px 0;">{sev_icon} {anomaly['type']} - {anomaly['column']}</h4>
            <p style="margin: 5px 0;"><b>{anomaly['description']}</b></p>
            <p style="margin: 5px 0; font-size: 12px; color: #666;">
                {', '.join([f"{k}: {v}" for k, v in anomaly['details'].items()])}
            </p>
        </div>
        """
    
    # Add visualization section if available
    viz_section = ""
    if bias_viz_data:
        viz_section = """
        <hr style="margin: 20px 0;">
        <h3>📊 Current Bias Metrics Visualization</h3>
        <p style="color: #666; font-size: 12px; margin-bottom: 10px;">
            Latest bias analysis from the current run:
        </p>
        <img src="cid:bias_viz" style="max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px;" />
        """
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto;">
        <h2>{severity_emoji} Data Quality Anomaly Detected</h2>
        
        <div style="margin: 20px 0; background-color: #f5f5f5; padding: 15px; border-radius: 4px;">
            <p style="margin: 5px 0;"><b>Severity:</b> <span style="color: {'#d32f2f' if anomalies['severity'] == 'critical' else '#f57c00'};">{anomalies['severity'].upper()}</span></p>
            <p style="margin: 5px 0;"><b>Timestamp:</b> {anomalies['timestamp']}</p>
            <p style="margin: 5px 0;"><b>Total Papers:</b> {schema.get('total_papers', 'N/A'):,}</p>
            <p style="margin: 5px 0;"><b>Quality Score:</b> {schema.get('quality_metrics', {}).get('overall_score', 'N/A')}%</p>
        </div>
        
        <h3>🚨 Detected Anomalies ({len(anomalies['detected_anomalies'])}):</h3>
        {anomaly_details}
        
        {viz_section}
        
        <hr style="margin: 20px 0;">
        <div style="background-color: #fff3e0; padding: 15px; border-radius: 4px; border-left: 4px solid #ff9800;">
            <h4 style="margin-top: 0;">⚡ Action Required</h4>
            <p style="margin: 5px 0;">Review the data pipeline and investigate the root cause of these anomalies.</p>
            <ul style="margin: 10px 0;">
                <li>Check full schema report: <code>gs://{os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')}/schemas/</code></li>
                <li>Review bias visualizations: <code>gs://{os.getenv('GCS_BUCKET_NAME', 'citeconnect-test-bucket')}/databias_v2/visualizations/</code></li>
                <li>Compare with previous runs to identify trends</li>
            </ul>
        </div>
        
        <p style="color: #999; font-size: 11px; margin-top: 30px; text-align: center;">
            CiteConnect Data Quality Monitoring System
        </p>
    </body>
    </html>
    """
    
    # Send email
    try:
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = alert_email
        
        # Attach HTML body
        msg_alternative = MIMEMultipart('alternative')
        msg.attach(msg_alternative)
        
        html_part = MIMEText(html_body, 'html')
        msg_alternative.attach(html_part)
        
        # Attach visualization as inline image if available
        if bias_viz_data:
            img = MIMEImage(bias_viz_data)
            img.add_header('Content-ID', '<bias_viz>')
            img.add_header('Content-Disposition', 'inline', filename='bias_metrics.png')
            msg.attach(img)
            logger.info("✅ Attached bias visualization to email")
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"✅ Anomaly alert email sent to {alert_email}")
        print(f"✅ Anomaly alert email sent to {alert_email}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send anomaly alert email: {e}")
        print(f"❌ Failed to send anomaly alert email: {e}")


def validate_schema(**context):
    """Main validation function called from DAG with anomaly detection"""
    import pandas as pd
    from pathlib import Path
    from google.cloud import storage
    
    print("="*60)
    print("SCHEMA VALIDATION & ANOMALY DETECTION")
    print("="*60)
    
    # Get data from previous task
    ti = context['task_instance']
    
    # Try to find processed data from GCS
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    df = None
    
    if bucket_name:
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            
            # List processed files
            blobs = list(bucket.list_blobs(prefix='processed_v2/'))
            if blobs:
                # Get latest processed file
                latest_blob = max(blobs, key=lambda b: b.time_created)
                
                # Download to temp location
                temp_path = f"/tmp/{latest_blob.name.split('/')[-1]}"
                latest_blob.download_to_filename(temp_path)
                
                df = pd.read_parquet(temp_path)
                print(f"✅ Loaded {len(df)} papers from GCS: {latest_blob.name}")
                
                # Clean up temp file
                os.remove(temp_path)
        except Exception as e:
            print(f"⚠️ Could not load from GCS: {e}")
    
    # Fallback to local paths
    if df is None:
        possible_paths = [
            "/opt/airflow/working_data/processed",
            "/tmp/test_data/processed",
            "data/preprocessed"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                parquet_files = list(Path(path).glob("*.parquet"))
                if parquet_files:
                    data_path = str(max(parquet_files, key=os.path.getmtime))
                    df = pd.read_parquet(data_path)
                    print(f"✅ Loaded {len(df)} papers from {data_path}")
                    break
    
    if df is None or df.empty:
        print("⚠️ No data found, skipping validation")
        return {'status': 'skipped', 'reason': 'no_data'}
    
    # Initialize validator with anomaly detection
    validator = SchemaValidator()
    
    # Run validation with anomaly detection
    schema = validator.validate_with_anomaly_detection(df)
    
    # Save schema locally
    schema_file = validator.save_schema(schema)
    print(f"✅ Schema saved to: {schema_file}")
    
    # Upload to GCS
    if bucket_name:
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            gcs_path = f"schemas/{Path(schema_file).name}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(schema_file)
            print(f"✅ Schema uploaded to GCS: gs://{bucket_name}/{gcs_path}")
        except Exception as e:
            print(f"⚠️ GCS upload failed (non-critical): {e}")
    
    # Compare with previous (quality drop check)
    comparison = validator.compare_with_previous(schema)
    
    # Print quality metrics
    quality = schema['quality_metrics']
    print(f"\n📊 Quality Metrics:")
    print(f"  - Completeness: {quality['completeness']}%")
    print(f"  - Validity: {quality['validity']}%")
    print(f"  - Overall Score: {quality['overall_score']}%")
    
    # Print anomaly detection results
    anomalies = schema.get('anomalies', {})
    alert_message = None
    
    if anomalies.get('has_anomalies'):
        severity = anomalies['severity']
        severity_emoji = "🔴" if severity == 'critical' else "⚠️"
        
        print(f"\n{severity_emoji} ANOMALIES DETECTED (Severity: {severity.upper()})")
        print(f"   Total anomalies: {len(anomalies['detected_anomalies'])}")
        
        for anomaly in anomalies['detected_anomalies']:
            sev_icon = "🔴" if anomaly['severity'] == 'critical' else "⚠️"
            print(f"\n   {sev_icon} {anomaly['type']} in '{anomaly['column']}'")
            print(f"      {anomaly['description']}")
            print(f"      Details: {anomaly['details']}")
        
        alert_message = f"Detected {len(anomalies['detected_anomalies'])} anomalies (severity: {severity})"
        
        # Send email alert for critical anomalies
        if severity in ['critical', 'warning']:
            print(f"\n📧 Sending anomaly alert email...")
            send_anomaly_alert(anomalies, schema)
    else:
        print("\n✅ No anomalies detected - data quality looks good!")
    
    # Alert if quality dropped (existing logic)
    if comparison.get('quality_dropped'):
        print("\n⚠️ QUALITY DROP DETECTED:")
        for metric, details in comparison['drop_details'].items():
            print(f"  - {metric}: {details['current']}% (was {details['previous']}%, dropped {details['drop']}%)")
            if not alert_message:
                alert_message = f"Quality dropped: {metric} down by {details['drop']}%"
    
    print("="*60)
    
    return {
        'status': 'completed',
        'schema_file': schema_file,
        'total_papers': len(df),
        'quality_score': quality['overall_score'],
        'has_anomalies': anomalies.get('has_anomalies', False),
        'anomaly_severity': anomalies.get('severity', 'normal'),
        'anomaly_count': len(anomalies.get('detected_anomalies', [])),
        'alert': alert_message
    }

