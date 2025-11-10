"""
GCS Reader Utilities
Reads parquet files from Google Cloud Storage buckets or local filesystem.
"""

import logging
import pandas as pd
from typing import List, Optional, Generator
import tempfile
import os
import glob

logger = logging.getLogger(__name__)


class GCSReader:
    """Read parquet files from Google Cloud Storage."""

    def __init__(self, bucket_name: str = "citeconnect-processed-parquet", project_id: Optional[str] = None):
        """
        Initialize GCS reader.

        Args:
            bucket_name: GCS bucket name (default: citeconnect-processed-parquet)
            project_id: GCP project ID (optional, for cross-project access)
        """
        from google.cloud import storage
        import os
        
        self.bucket_name = bucket_name
        
        # Check if credentials file is set
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path:
            logger.info(f"🔑 Using credentials from: {creds_path}")
        
        try:
            # Initialize client with specific project if provided
            if project_id:
                self.client = storage.Client(project=project_id)
                logger.info(f"✅ Connected to GCS using project: {project_id}")
            else:
                # Let SDK auto-detect project from credentials
                self.client = storage.Client()
                logger.info(f"✅ Connected to GCS with default project")
            
            self.bucket = self.client.bucket(bucket_name)
            
            # Try to verify bucket access
            try:
                bucket_exists = self.bucket.exists()
                if bucket_exists:
                    logger.info(f"✅ Verified bucket exists: {bucket_name}")
                else:
                    logger.warning(f"⚠️ Bucket may not exist: {bucket_name}")
            except Exception as verify_error:
                logger.warning(f"⚠️ Could not verify bucket: {verify_error}")
            
            logger.info(f"✅ Connected to GCS bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to GCS: {e}")
            raise

    def list_parquet_files(self, prefix: str = "") -> List[str]:
        """
        List all parquet files in bucket with optional prefix.

        Args:
            prefix: Folder prefix (e.g., 'raw/healthcare/')

        Returns:
            List of blob paths
        """
        try:
            logger.info(f"Current prefix is {prefix}...")    
            blobs = self.bucket.list_blobs(prefix=prefix)
            parquet_files = [
                blob.name for blob in blobs 
                if blob.name.endswith('.parquet')
            ]
            logger.info(f"Found {len(parquet_files)} parquet files in gs://{self.bucket_name}/{prefix}")
            return parquet_files
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []

    def read_parquet(self, blob_path: str) -> Optional[pd.DataFrame]:
        """
        Read single parquet file from GCS into DataFrame.

        Args:
            blob_path: Path to parquet file in bucket (e.g., 'raw/healthcare/AI_in_healthcare.parquet')

        Returns:
            DataFrame or None on failure
        """
        try:
            logger.info(f"📥 Attempting to download: gs://{self.bucket_name}/{blob_path}")
            blob = self.bucket.blob(blob_path)
            
            # Check if blob exists before downloading
            if not blob.exists():
                logger.error(f"❌ Blob does not exist: gs://{self.bucket_name}/{blob_path}")
                return None
            
            logger.debug(f"Blob exists, downloading to temp file...")
            
            # Download to temporary file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                logger.debug(f"Temp file: {tmp_file.name}")
                blob.download_to_filename(tmp_file.name)  # ← 403 ERROR HAPPENS HERE
                df = pd.read_parquet(tmp_file.name)
                
            # Clean up temp file
            os.unlink(tmp_file.name)
            
            logger.info(f"✅ Read {len(df)} records from gs://{self.bucket_name}/{blob_path}")
            return df

        except Exception as e:
            logger.error(
                f"❌ Failed to read {blob_path}: {e}",
                exc_info=True  # This adds full stack trace
            )
            return None

    def read_all_from_domain(self, domain: str, custom_prefix: Optional[str] = None, flat_structure: bool = False) -> pd.DataFrame:
        """
        Read all parquet files from a domain folder and concatenate.
        
        ⚠️ MEMORY WARNING: Loads all data into memory at once.
        For large datasets (>10k papers), use stream_parquet_batches() instead.

        Args:
            domain: Domain folder name (e.g., 'healthcare', 'finance')
            custom_prefix: Override default prefix (e.g., 'raw/' for flat structure)
            flat_structure: If True, ignore domain and search directly in custom_prefix

        Returns:
            Concatenated DataFrame
        """
        # Handle flat structure (all files in one folder)
        if flat_structure and custom_prefix:
            prefix = custom_prefix
            logger.info(f"Using flat structure: searching all files in {prefix}")
        # Handle custom prefix with domain
        elif custom_prefix:
            prefix = custom_prefix
        # Default: domain-based structure
        else:
            prefix = f"raw/{domain}/"
            
        parquet_files = self.list_parquet_files(prefix)

        if not parquet_files:
            logger.warning(f"No parquet files found in {prefix}")
            return pd.DataFrame()

        dataframes = []
        for blob_path in parquet_files:
            df = self.read_parquet(blob_path)
            if df is not None:
                dataframes.append(df)

        if not dataframes:
            logger.warning(f"Failed to read any files from {prefix}")
            return pd.DataFrame()

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"✅ Combined {len(dataframes)} files → {len(combined_df)} total records")
        return combined_df

    def stream_parquet_batches(
        self, 
        domain: str, 
        batch_size: int = 100,
        custom_prefix: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Stream parquet files in batches (memory-efficient).
        
        This is a GENERATOR function for large-scale processing.
        Yields DataFrames in chunks to avoid loading entire dataset into memory.

        Args:
            domain: Domain folder name
            batch_size: Records per batch
            custom_prefix: Override default prefix

        Yields:
            DataFrame batches
        
        Example usage:
            reader = GCSReader()
            for batch_df in reader.stream_parquet_batches('healthcare', batch_size=50):
                process_batch(batch_df)
        """
        # Allow custom prefix
        if custom_prefix:
            prefix = custom_prefix
        else:
            prefix = f"raw/"
            
        parquet_files = self.list_parquet_files(prefix)

        total_batches = 0
        for blob_path in parquet_files:
            df = self.read_parquet(blob_path)
            if df is not None:
                # Yield in batches
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]
                    total_batches += 1
                    logger.debug(f"Yielding batch {total_batches} with {len(batch_df)} records")
                    yield batch_df

        logger.info(f"Streamed {total_batches} total batches from {domain}")


class LocalFileReader:
    """Fallback reader for local parquet files (development/testing)."""

    def __init__(self, base_dir: str = "data"):
        """
        Initialize local reader.

        Args:
            base_dir: Base directory for local parquet files (default: 'data')
        """
        self.base_dir = base_dir
        logger.info(f"📁 Using local file reader: {base_dir}")

    def list_parquet_files(self, domain: str) -> List[str]:
        """
        List all parquet files in local domain folder.

        Args:
            domain: Domain folder name (e.g., 'healthcare', 'finance')

        Returns:
            List of file paths
        """
        pattern = f"{self.base_dir}/{domain}/*.parquet"
        files = glob.glob(pattern)
        
        logger.info(f"Found {len(files)} parquet files in {pattern}")
        return files

    def read_parquet(self, filepath: str) -> Optional[pd.DataFrame]:
        """
        Read single parquet file from local filesystem.

        Args:
            filepath: Path to local parquet file

        Returns:
            DataFrame or None on failure
        """
        try:
            if not os.path.exists(filepath):
                logger.error(f"File not found: {filepath}")
                return None
            
            df = pd.read_parquet(filepath)
            logger.info(f"✅ Read {len(df)} records from {filepath}")
            return df

        except Exception as e:
            logger.error(f"❌ Failed to read {filepath}: {e}")
            return None

    def read_all_from_domain(self, domain: str, custom_prefix: Optional[str] = None) -> pd.DataFrame:
        """
        Read all parquet files from local domain folder and concatenate.

        Args:
            domain: Domain folder name (e.g., 'healthcare', 'finance')
            custom_prefix: Not used for local reader (for API compatibility)

        Returns:
            Concatenated DataFrame
        """
        pattern = f"{self.base_dir}/{domain}/*.parquet"
        files = glob.glob(pattern)

        if not files:
            logger.warning(f"No local files found matching {pattern}")
            logger.info(f"💡 Make sure parquet files exist in: {os.path.abspath(self.base_dir)}/{domain}/")
            return pd.DataFrame()

        logger.info(f"Found {len(files)} parquet files in {self.base_dir}/{domain}/")
        
        dataframes = []
        for filepath in files:
            df = self.read_parquet(filepath)
            if df is not None:
                dataframes.append(df)

        if not dataframes:
            logger.warning(f"Failed to read any files from {pattern}")
            return pd.DataFrame()

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"✅ Combined {len(dataframes)} files → {len(combined_df)} total records")
        return combined_df

    def stream_parquet_batches(
        self, 
        domain: str, 
        batch_size: int = 100,
        custom_prefix: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Stream local parquet files in batches (memory-efficient).

        Args:
            domain: Domain folder name
            batch_size: Records per batch
            custom_prefix: Not used for local reader

        Yields:
            DataFrame batches
        """
        files = self.list_parquet_files(domain)

        total_batches = 0
        for filepath in files:
            df = self.read_parquet(filepath)
            if df is not None:
                # Yield in batches
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]
                    total_batches += 1
                    logger.debug(f"Yielding batch {total_batches} with {len(batch_df)} records")
                    yield batch_df

        logger.info(f"Streamed {total_batches} total batches from {domain}")