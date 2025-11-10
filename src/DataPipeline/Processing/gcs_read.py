import pandas as pd
import glob
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

def load_data_from_source(input_path: str) -> pd.DataFrame:
    if input_path.startswith('gs://'):
        if not GCS_AVAILABLE:
            raise ImportError("google-cloud-storage required for GCS paths. Install: pip install google-cloud-storage")
        
        logger.info(f"Loading from GCS: {input_path}")
        
        # Parse bucket and prefix
        parts = input_path.replace('gs://', '').split('/', 1)
        print(parts, "BUCKET AND PREFIX")
        bucket_name = parts[0]
        print(bucket_name, "BUCKET NAME")
        prefix = parts[1] if len(parts) > 1 else ''
        
        # List parquet files in GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        parquet_blobs = [b for b in blobs if b.name.endswith('.parquet') and not b.name.endswith('_metadata.json')]
        
        if not parquet_blobs:
            raise FileNotFoundError(f"No parquet files found in {input_path}")
        
        logger.info(f"Found {len(parquet_blobs)} parquet files in GCS")
        
        # Read each file from GCS
        dfs = []
        for blob in parquet_blobs:
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            df = pd.read_parquet(gcs_uri)
            dfs.append(df)
            logger.info(f"Loaded {blob.name}: {len(df)} papers")
        
        return pd.concat(dfs, ignore_index=True)
    
    else:
        # Local file system
        logger.info(f"Loading from local: {input_path}")
        files = glob.glob(f'{input_path}/*.parquet')
        
        if not files:
            raise FileNotFoundError(f"No parquet files found in {input_path}")
        
        logger.info(f"Found {len(files)} parquet files locally")
        
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True)