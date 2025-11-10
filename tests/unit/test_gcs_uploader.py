"""
Unit tests for gcs_uploader.py

Tests cover:
- Successful GCS uploads
- Error handling for upload failures
- Client and bucket initialization
- Logging behavior
- Edge cases and malformed inputs
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from google.cloud import storage

from src.DataPipeline.Ingestion.gcs_uploader import upload_to_gcs


class TestGCSUploadSuccess:
    """Test successful GCS upload scenarios."""

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_successful_upload_returns_true(self, mock_client_class):
        """Test that successful upload returns True."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.return_value = None  # Successful upload
        
        # Act
        result = upload_to_gcs(
            local_path="/tmp/test.parquet",
            bucket_name="test-bucket",
            destination_path="papers/test.parquet"
        )
        
        # Assert
        assert result is True

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_client_initialization_called(self, mock_client_class):
        """Test that storage.Client() is initialized."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Act
        upload_to_gcs("/tmp/test.parquet", "bucket", "path/file.parquet")
        
        # Assert
        mock_client_class.assert_called_once()

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_bucket_accessed_with_correct_name(self, mock_client_class):
        """Test that bucket is accessed with correct name."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        bucket_name = "my-test-bucket"
        
        # Act
        upload_to_gcs("/tmp/test.parquet", bucket_name, "path/file.parquet")
        
        # Assert
        mock_client.bucket.assert_called_once_with(bucket_name)

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_blob_created_with_correct_path(self, mock_client_class):
        """Test that blob is created with correct destination path."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        destination_path = "papers/data/test.parquet"
        
        # Act
        upload_to_gcs("/tmp/test.parquet", "bucket", destination_path)
        
        # Assert
        mock_bucket.blob.assert_called_once_with(destination_path)

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_upload_from_filename_called(self, mock_client_class):
        """Test that upload_from_filename is called with local path."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        local_path = "/tmp/my_data.parquet"
        
        # Act
        upload_to_gcs(local_path, "bucket", "destination.parquet")
        
        # Assert
        mock_blob.upload_from_filename.assert_called_once_with(local_path)


class TestGCSUploadFailures:
    """Test GCS upload failure scenarios."""

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_upload_failure_returns_false(self, mock_client_class):
        """Test that upload failure returns False."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Simulate upload failure
        mock_blob.upload_from_filename.side_effect = Exception("Upload failed")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_client_initialization_error(self, mock_client_class):
        """Test handling of client initialization errors."""
        # Arrange
        mock_client_class.side_effect = Exception("Authentication failed")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_bucket_access_error(self, mock_client_class):
        """Test handling of bucket access errors."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.bucket.side_effect = Exception("Bucket not found")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_blob_creation_error(self, mock_client_class):
        """Test handling of blob creation errors."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.side_effect = Exception("Invalid blob path")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_file_not_found_error(self, mock_client_class):
        """Test handling when local file doesn't exist."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Simulate file not found
        mock_blob.upload_from_filename.side_effect = FileNotFoundError("File not found")
        
        # Act
        result = upload_to_gcs("/nonexistent/file.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False


class TestGCSUploadLogging:
    """Test logging behavior during GCS operations."""

    @patch('src.DataPipeline.Ingestion.gcs_uploader.logging')
    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_logs_successful_upload(self, mock_client_class, mock_logging):
        """Test that successful upload is logged."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        bucket_name = "test-bucket"
        destination_path = "papers/test.parquet"
        
        # Act
        upload_to_gcs("/tmp/test.parquet", bucket_name, destination_path)
        
        # Assert
        mock_logging.info.assert_called_once()
        call_args = mock_logging.info.call_args[0][0]
        assert bucket_name in call_args
        assert destination_path in call_args

    @patch('src.DataPipeline.Ingestion.gcs_uploader.logging')
    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_logs_upload_failure(self, mock_client_class, mock_logging):
        """Test that upload failure is logged."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        error_message = "Network error"
        mock_blob.upload_from_filename.side_effect = Exception(error_message)
        
        # Act
        upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        mock_logging.error.assert_called_once()
        call_args = mock_logging.error.call_args[0][0]
        assert "GCS upload failed" in call_args


class TestGCSUploadEdgeCases:
    """Test edge cases and unusual scenarios."""

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_empty_bucket_name(self, mock_client_class):
        """Test handling of empty bucket name."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.bucket.side_effect = Exception("Invalid bucket name")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "", "path.parquet")
        
        # Assert
        assert result is False

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_empty_destination_path(self, mock_client_class):
        """Test handling of empty destination path."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "")
        
        # Assert
        # Should attempt to create blob with empty path
        mock_bucket.blob.assert_called_once_with("")

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_special_characters_in_path(self, mock_client_class):
        """Test handling of special characters in destination path."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        destination_path = "papers/data/test file (2023).parquet"
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", destination_path)
        
        # Assert
        assert result is True
        mock_bucket.blob.assert_called_once_with(destination_path)

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_nested_destination_paths(self, mock_client_class):
        """Test handling of deeply nested destination paths."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        nested_path = "level1/level2/level3/data.parquet"
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", nested_path)
        
        # Assert
        assert result is True
        mock_bucket.blob.assert_called_once_with(nested_path)


class TestGCSUploadReturnValues:
    """Test return value behavior."""

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_returns_boolean_on_success(self, mock_client_class):
        """Test that function returns boolean True on success."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is True
        assert isinstance(result, bool)

    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_returns_boolean_on_failure(self, mock_client_class):
        """Test that function returns boolean False on failure."""
        # Arrange
        mock_client_class.side_effect = Exception("Error")
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False
        assert isinstance(result, bool)


class TestGCSUploadExceptionTypes:
    """Test handling of different exception types."""

    @pytest.mark.parametrize("exception_type,exception_msg", [
        (FileNotFoundError, "File not found"),
        (PermissionError, "Permission denied"),
        (ValueError, "Invalid value"),
        (ConnectionError, "Network error"),
        (Exception, "Generic error")
    ])
    @patch('src.DataPipeline.Ingestion.gcs_uploader.storage.Client')
    def test_handles_various_exception_types(self, mock_client_class, exception_type, exception_msg):
        """Test that different exception types are handled gracefully."""
        # Arrange
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        mock_blob.upload_from_filename.side_effect = exception_type(exception_msg)
        
        # Act
        result = upload_to_gcs("/tmp/test.parquet", "bucket", "path.parquet")
        
        # Assert
        assert result is False