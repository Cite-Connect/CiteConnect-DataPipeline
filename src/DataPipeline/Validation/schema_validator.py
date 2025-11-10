"""
Schema Validation Module for CiteConnect
Validates schema and data quality with alerting on quality drops
"""

import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates data schema and tracks quality over time"""
    
    def __init__(self, schema_dir: str = "data/schemas"):
        self.schema_dir = Path(schema_dir)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        
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


def validate_schema(**context):
    """Main validation function called from DAG"""
    import pandas as pd
    from pathlib import Path
    
    print("="*60)
    print("SCHEMA VALIDATION & QUALITY TRACKING")
    print("="*60)
    
    # Get data from previous task
    ti = context['task_instance']
    
    # Try to find processed data
    data_path = None
    possible_paths = [
        "/tmp/test_data/processed",
        "/opt/airflow/data/preprocessed",
        "data/preprocessed"
    ]
    
    df = None
    for path in possible_paths:
        if os.path.exists(path):
            if path.endswith('.parquet'):
                data_path = path
            else:
                parquet_files = list(Path(path).glob("*.parquet"))
                if parquet_files:
                    data_path = str(parquet_files[-1])
            
            if data_path and os.path.exists(data_path):
                df = pd.read_parquet(data_path)
                print(f"✅ Loaded {len(df)} papers from {data_path}")
                break
    
    if df is None or df.empty:
        print("⚠️ No data found, skipping validation")
        return {'status': 'skipped'}
    
    # Initialize validator
    validator = SchemaValidator()
    
    # Generate and validate
    schema = validator.generate_and_validate(df)
    
    # Save schema locally
    schema_file = validator.save_schema(schema)
    print(f"✅ Schema saved to: {schema_file}")
    
    # Upload to GCS if credentials available
    try:
        from google.cloud import storage
        
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        if bucket_name:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            gcs_path = f"schemas/{Path(schema_file).name}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(schema_file)
            print(f"✅ Schema uploaded to GCS: gs://{bucket_name}/{gcs_path}")
    except Exception as e:
        print(f"⚠️ GCS upload failed (non-critical): {e}")
    
    # Compare with previous
    comparison = validator.compare_with_previous(schema)
    
    # Print quality metrics
    quality = schema['quality_metrics']
    print(f"\n📊 Quality Metrics:")
    print(f"  - Completeness: {quality['completeness']}%")
    print(f"  - Validity: {quality['validity']}%")
    print(f"  - Overall Score: {quality['overall_score']}%")
    
    # Alert if quality dropped
    alert_message = None
    if comparison.get('quality_dropped'):
        print("\n❌ ALERT: Content quality has dropped!")
        for metric, details in comparison['drop_details'].items():
            print(f"  - {metric}: {details['current']}% (was {details['previous']}%)")
            alert_message = f"Quality dropped: {metric} down by {details['drop']}%"
    
    print("="*60)
    
    return {
        'status': 'completed',
        'schema_file': schema_file,
        'total_papers': len(df),
        'quality_score': quality['overall_score'],
        'alert': alert_message
    }

