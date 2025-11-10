"""
Unit tests for semantic_scholar_client.py

Tests cover:
- Successful API responses
- Error handling and retries
- Rate limiting
- Field extraction
- Edge cases and failures
"""
import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
import time

from src.DataPipeline.Ingestion.semantic_scholar_client import search_semantic_scholar


class TestSearchSemanticScholar:
    """Test suite for search_semantic_scholar function."""

    # ========================================================================
    # SUCCESSFUL RESPONSE TESTS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_successful_search_returns_papers(self, mock_sleep, mock_get):
        """Test that successful API call returns paper data."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {
                    'paperId': 'abc123',
                    'title': 'Test Paper',
                    'abstract': 'Test abstract',
                    'year': 2023
                }
            ]
        }
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("machine learning", limit=10)

        # Assert
        assert len(result) == 1
        assert result[0]['paperId'] == 'abc123'
        assert result[0]['title'] == 'Test Paper'
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()  # No retry needed

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_successful_search_with_multiple_papers(self, mock_sleep, mock_get):
        """Test that multiple papers are returned correctly."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {'paperId': f'paper{i}', 'title': f'Paper {i}'} 
                for i in range(5)
            ]
        }
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("deep learning", limit=5)

        # Assert
        assert len(result) == 5
        assert all('paperId' in paper for paper in result)

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_empty_results_returns_empty_list(self, mock_sleep, mock_get):
        """Test that empty API response returns empty list."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("nonexistent topic")

        # Assert
        assert result == []
        assert isinstance(result, list)

    # ========================================================================
    # URL CONSTRUCTION TESTS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_url_contains_correct_query(self, mock_sleep, mock_get):
        """Test that URL is constructed with correct query parameter."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        query = "neural networks"
        
        # Act
        search_semantic_scholar(query, limit=10)

        # Assert
        called_url = mock_get.call_args[0][0]
        assert f"query={query}" in called_url
        assert "limit=10" in called_url

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_url_contains_all_required_fields(self, mock_sleep, mock_get):
        """Test that URL includes all required metadata fields."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        required_fields = [
            'paperId', 'title', 'abstract', 'year', 'authors',
            'citationCount', 'openAccessPdf', 'externalIds'
        ]

        # Act
        search_semantic_scholar("test query")

        # Assert
        called_url = mock_get.call_args[0][0]
        for field in required_fields:
            assert field in called_url

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_custom_limit_parameter(self, mock_sleep, mock_get):
        """Test that custom limit is applied correctly."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        search_semantic_scholar("test", limit=50)

        # Assert
        called_url = mock_get.call_args[0][0]
        assert "limit=50" in called_url

    # ========================================================================
    # ERROR HANDLING & RETRY TESTS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_retry_on_failed_request(self, mock_sleep, mock_get):
        """Test that function retries on failed requests."""
        # Arrange
        mock_get.side_effect = [
            Mock(status_code=500),  # First attempt fails
            Mock(status_code=500),  # Second attempt fails
            Mock(status_code=200, json=lambda: {'data': [{'paperId': 'success'}]})  # Third succeeds
        ]

        # Act
        result = search_semantic_scholar("test query", retries=3)

        # Assert
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2  # Sleep after first two failures
        assert len(result) == 1
        assert result[0]['paperId'] == 'success'

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_returns_empty_list_after_all_retries_exhausted(self, mock_sleep, mock_get):
        """Test that empty list is returned when all retries fail."""
        # Arrange
        mock_get.side_effect = [
            Mock(status_code=500),
            Mock(status_code=500),
            Mock(status_code=500)
        ]

        # Act
        result = search_semantic_scholar("test query", retries=3)

        # Assert
        assert result == []
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 3

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_retry_on_timeout_exception(self, mock_sleep, mock_get):
        """Test retry behavior on timeout exceptions."""
        # Arrange
        mock_get.side_effect = [
            requests.exceptions.Timeout("Timeout"),
            Mock(status_code=200, json=lambda: {'data': [{'paperId': 'recovered'}]})
        ]

        # Act
        result = search_semantic_scholar("test query", retries=3)

        # Assert
        assert mock_get.call_count == 2
        assert len(result) == 1
        assert result[0]['paperId'] == 'recovered'

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_retry_on_connection_error(self, mock_sleep, mock_get):
        """Test retry behavior on connection errors."""
        # Arrange
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            Mock(status_code=200, json=lambda: {'data': []})
        ]

        # Act
        result = search_semantic_scholar("test query", retries=3)

        # Assert
        assert mock_get.call_count == 2
        assert result == []

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_sleep_duration_between_retries(self, mock_sleep, mock_get):
        """Test that sleep duration is correct between retries."""
        # Arrange
        mock_get.side_effect = [
            Mock(status_code=500),
            Mock(status_code=500),
            Mock(status_code=200, json=lambda: {'data': []})
        ]

        # Act
        search_semantic_scholar("test query", retries=3)

        # Assert
        # Should sleep after first two failures
        assert mock_sleep.call_count == 2
        # Verify sleep duration is 3 seconds
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 3

    # ========================================================================
    # HTTP STATUS CODE TESTS
    # ========================================================================

    @pytest.mark.parametrize("status_code", [400, 403, 404, 429, 500, 502, 503])
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_non_200_status_codes_trigger_retry(self, mock_sleep, mock_get, status_code):
        """Test that non-200 status codes trigger retry logic."""
        # Arrange
        mock_get.return_value = Mock(status_code=status_code)

        # Act
        result = search_semantic_scholar("test", retries=3)

        # Assert
        assert result == []
        assert mock_get.call_count == 3  # All retries attempted

    # ========================================================================
    # EDGE CASES & SPECIAL SCENARIOS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_special_characters_in_query(self, mock_sleep, mock_get):
        """Test that special characters in query are handled."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        query = "AI & ML: neural networks (2023)"
        result = search_semantic_scholar(query)

        # Assert
        assert result == []
        assert mock_get.called

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_empty_query_string(self, mock_sleep, mock_get):
        """Test behavior with empty query string."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("")

        # Assert
        assert result == []
        assert mock_get.called

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_zero_limit_parameter(self, mock_sleep, mock_get):
        """Test behavior with limit=0."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("test", limit=0)

        # Assert
        called_url = mock_get.call_args[0][0]
        assert "limit=0" in called_url

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_malformed_json_response(self, mock_sleep, mock_get):
        """Test handling of malformed JSON response."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("test", retries=2)

        # Assert
        assert result == []
        assert mock_get.call_count == 2

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_response_missing_data_field(self, mock_sleep, mock_get):
        """Test handling when response doesn't contain 'data' field."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'error': 'Something went wrong'}
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("test")

        # Assert
        assert result == []

    # ========================================================================
    # HEADERS & TIMEOUT TESTS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_request_includes_headers(self, mock_sleep, mock_get):
        """Test that request includes proper headers."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        search_semantic_scholar("test")

        # Assert
        call_kwargs = mock_get.call_args[1]
        assert 'headers' in call_kwargs
        assert call_kwargs['headers'] is not None

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_request_includes_timeout(self, mock_sleep, mock_get):
        """Test that request includes timeout parameter."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        # Act
        search_semantic_scholar("test")

        # Assert
        call_kwargs = mock_get.call_args[1]
        assert 'timeout' in call_kwargs
        assert call_kwargs['timeout'] == 40

    # ========================================================================
    # INTEGRATION-LIKE TESTS (with realistic mock data)
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_realistic_paper_data_structure(self, mock_sleep, mock_get):
        """Test with realistic paper data structure from API."""
        # Arrange
        realistic_paper = {
            'paperId': '204e3073870fae3d05bcbc2f6a8e263d9b72e776',
            'externalIds': {'ArXiv': '1706.03762', 'DOI': '10.5555/3295222'},
            'title': 'Attention Is All You Need',
            'abstract': 'The dominant sequence transduction models...',
            'year': 2017,
            'publicationDate': '2017-06-12',
            'authors': [
                {'authorId': '1234', 'name': 'Ashish Vaswani'}
            ],
            'venue': 'NeurIPS',
            'citationCount': 89234,
            'influentialCitationCount': 12456,
            'referenceCount': 42,
            'isOpenAccess': True,
            'openAccessPdf': {
                'url': 'https://arxiv.org/pdf/1706.03762.pdf',
                'status': 'GREEN'
            },
            'tldr': {
                'model': 'tldr@v2.0.0',
                'text': 'The Transformer model.'
            }
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [realistic_paper]}
        mock_get.return_value = mock_response

        # Act
        result = search_semantic_scholar("transformers")

        # Assert
        assert len(result) == 1
        paper = result[0]
        assert paper['paperId'] == '204e3073870fae3d05bcbc2f6a8e263d9b72e776'
        assert 'ArXiv' in paper['externalIds']
        assert paper['citationCount'] == 89234
        assert paper['isOpenAccess'] is True

    # ========================================================================
    # PERFORMANCE TESTS
    # ========================================================================

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_single_retry_completes_quickly(self, mock_sleep, mock_get):
        """Test that single retry doesn't take excessive time."""
        # Arrange
        mock_get.side_effect = [
            Mock(status_code=500),
            Mock(status_code=200, json=lambda: {'data': []})
        ]

        # Act
        start = time.time()
        search_semantic_scholar("test", retries=2)
        duration = time.time() - start

        # Assert - should complete in under 1 second (mocked sleep)
        assert duration < 1.0
        assert mock_sleep.call_count == 1