"""
End-to-End Integration Tests for CiteConnect Pipeline

Tests the complete pipeline flow:
1. Semantic Scholar API → Paper retrieval
2. Metadata extraction → Structured data
3. Content extraction → Introduction text
4. Processing orchestration → Complete records

These tests use realistic mock data to validate the full workflow.
"""
import pytest
from unittest.mock import Mock, patch
import json

from src.DataPipeline.Ingestion.semantic_scholar_client import search_semantic_scholar
from src.DataPipeline.Ingestion.metadata_utils import extract_metadata
from src.DataPipeline.Ingestion.content_extractor import ContentExtractor
from src.DataPipeline.Ingestion.processor import process_papers


# ============================================================================
# FIXTURES - Realistic Test Data
# ============================================================================

@pytest.fixture
def complete_paper():
    """Fixture providing a complete, realistic paper structure."""
    return {
        'paperId': '204e3073870fae3d05bcbc2f6a8e263d9b72e776',
        'externalIds': {
            'ArXiv': '1706.03762',
            'DOI': '10.5555/3295222.3295349'
        },
        'title': 'Attention Is All You Need',
        'abstract': 'The dominant sequence transduction models are based on complex recurrent or convolutional neural networks.',
        'year': 2017,
        'publicationDate': '2017-06-12',
        'authors': [
            {'authorId': '1234', 'name': 'Ashish Vaswani'},
            {'authorId': '5678', 'name': 'Noam Shazeer'}
        ],
        'citationCount': 89234,
        'referenceCount': 42,
        'fieldsOfStudy': ['Computer Science', 'Mathematics'],
        'openAccessPdf': {
            'url': 'https://arxiv.org/pdf/1706.03762.pdf'
        },
        'tldr': {
            'text': 'The Transformer model uses attention mechanisms.'
        }
    }


@pytest.fixture
def minimal_paper():
    """Fixture providing a minimal paper (only required fields)."""
    return {
        'paperId': 'minimal123',
        'title': 'Minimal Paper',
        'abstract': 'This is a minimal paper with only required fields.'
    }


@pytest.fixture
def paper_batch(complete_paper, minimal_paper):
    """Fixture providing a batch of mixed papers."""
    return [
        complete_paper,
        minimal_paper,
        {
            'paperId': 'paper3',
            'title': 'Third Paper',
            'abstract': 'Another test paper',
            'year': 2020,
            'authors': [{'name': 'Test Author'}],
            'citationCount': 10
        }
    ]


# ============================================================================
# TEST CLASS: API to Metadata Flow
# ============================================================================

class TestAPIToMetadataFlow:
    """Test the flow from API retrieval to metadata extraction."""

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_api_response_converts_to_metadata(self, mock_sleep, mock_get, complete_paper):
        """Test that API responses are correctly converted to metadata records."""
        # Arrange - Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [complete_paper]}
        mock_get.return_value = mock_response
        
        # Act - Retrieve from API
        papers = search_semantic_scholar("transformers", limit=1)
        
        # Convert to metadata
        metadata = extract_metadata(papers[0], "transformers")
        
        # Assert - Verify complete conversion
        assert metadata['paperId'] == complete_paper['paperId']
        assert metadata['title'] == complete_paper['title']
        assert metadata['search_term'] == 'transformers'
        assert metadata['year'] == 2017
        assert 'Ashish Vaswani' in metadata['authors']
        assert metadata['citationCount'] == 89234
        
        # Verify JSON serialization
        external_ids = json.loads(metadata['externalIds'])
        assert external_ids['ArXiv'] == '1706.03762'

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_multiple_papers_api_to_metadata(self, mock_sleep, mock_get, paper_batch):
        """Test batch conversion from API to metadata."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': paper_batch}
        mock_get.return_value = mock_response
        
        # Act
        papers = search_semantic_scholar("test", limit=3)
        metadata_records = [extract_metadata(p, "test") for p in papers]
        
        # Assert
        assert len(metadata_records) == 3
        assert all('paperId' in m for m in metadata_records)
        assert all('search_term' in m for m in metadata_records)
        assert all(m['search_term'] == 'test' for m in metadata_records)


# ============================================================================
# TEST CLASS: Complete Pipeline Flow
# ============================================================================

class TestCompletePipelineFlow:
    """Test the complete end-to-end pipeline."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_end_to_end_single_paper(self, mock_api_sleep, mock_api_get, 
                                      mock_extract_sleep, mock_process_sleep, 
                                      complete_paper):
        """Test complete pipeline with a single paper."""
        # Arrange - Mock API
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [complete_paper]}
        mock_api_get.return_value = mock_response
        
        # Act - Run complete pipeline
        # Step 1: Retrieve papers
        papers = search_semantic_scholar("transformers", limit=1)
        
        # Step 2: Process papers (includes metadata + content extraction)
        results = process_papers(papers, "transformers")
        
        # Assert - Verify complete pipeline output
        assert len(results) == 1
        result = results[0]
        
        # Verify metadata was extracted
        assert result['paperId'] == complete_paper['paperId']
        assert result['title'] == complete_paper['title']
        assert result['search_term'] == 'transformers'
        
        # Verify content extraction occurred
        assert 'introduction' in result
        assert 'extraction_method' in result
        assert 'content_quality' in result
        assert 'status' in result
        
        # Verify processing metadata
        assert 'scraped_at' in result
        assert result['scraped_at'].endswith('Z')

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_end_to_end_batch_processing(self, mock_api_sleep, mock_api_get, 
                                         mock_extract_sleep, mock_process_sleep, 
                                         paper_batch):
        """Test complete pipeline with multiple papers."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': paper_batch}
        mock_api_get.return_value = mock_response
        
        # Act
        papers = search_semantic_scholar("AI", limit=3)
        results = process_papers(papers, "AI")
        
        # Assert
        assert len(results) == 3
        
        # Verify all papers processed
        paper_ids = [r['paperId'] for r in results]
        assert len(set(paper_ids)) == 3  # All unique
        
        # Verify all have required fields
        required_fields = ['paperId', 'title', 'search_term', 'status', 'scraped_at']
        for result in results:
            for field in required_fields:
                assert field in result, f"Missing {field} in result"


# ============================================================================
# TEST CLASS: Data Quality Validation
# ============================================================================

class TestDataQualityValidation:
    """Test output data quality and schema compliance."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_output_schema_compliance(self, mock_extract_sleep, mock_process_sleep, 
                                      complete_paper):
        """Test that pipeline output complies with expected schema."""
        # Act
        results = process_papers([complete_paper], "test")
        result = results[0]
        
        # Assert - Verify all required fields exist
        required_fields = [
            'paperId', 'externalIds', 'title', 'abstract', 'year',
            'publicationDate', 'authors', 'citationCount', 'referenceCount',
            'fieldsOfStudy', 'pdf_url', 'tldr', 'introduction',
            'extraction_method', 'content_quality', 'has_intro',
            'intro_length', 'status', 'scraped_at', 'search_term'
        ]
        
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"
        
        # Verify data types
        assert isinstance(result['paperId'], str)
        assert isinstance(result['externalIds'], str)  # JSON string
        assert isinstance(result['citationCount'], int)
        assert isinstance(result['has_intro'], bool)
        assert isinstance(result['intro_length'], int)

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_all_papers_get_content(self, mock_extract_sleep, mock_process_sleep, 
                                    paper_batch):
        """Test that papers with abstracts get content from fallback."""
        # Act
        results = process_papers(paper_batch, "test")
        
        # Assert
        assert len(results) == len(paper_batch)
        
        # Papers with abstracts should get content (via fallback)
        papers_with_abstract = [r for r in results if r.get('abstract')]
        if papers_with_abstract:
            # At least papers with abstracts should have content
            assert any(r['introduction'] is not None for r in papers_with_abstract)

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_extraction_method_distribution(self, mock_extract_sleep, 
                                           mock_process_sleep, paper_batch):
        """Test that extraction is attempted for all papers."""
        # Act
        results = process_papers(paper_batch, "test")
        
        # Assert
        assert len(results) == len(paper_batch)
        
        # All papers should have status (even if extraction failed)
        assert all('status' in r for r in results)
        
        # Papers with successful extraction should have valid methods
        successful_results = [r for r in results if r.get('introduction')]
        if successful_results:
            valid_methods = ['arxiv_html', 'grobid_pdf', 'regex_pdf', 'abstract_tldr', 'tldr_only']
            for result in successful_results:
                assert result['extraction_method'] in valid_methods


# ============================================================================
# TEST CLASS: Error Handling & Robustness
# ============================================================================

class TestErrorHandlingRobustness:
    """Test pipeline behavior under error conditions."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_handles_empty_paper_list(self, mock_extract_sleep, mock_process_sleep):
        """Test pipeline handles empty input gracefully."""
        # Act
        results = process_papers([], "test")
        
        # Assert
        assert results == []
        assert isinstance(results, list)

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_handles_malformed_papers(self, mock_extract_sleep, mock_process_sleep):
        """Test pipeline handles malformed paper data gracefully."""
        # Arrange - Papers with various issues (but structurally valid)
        malformed_papers = [
            {'paperId': 'empty', 'title': 'Empty'},  # Minimal valid paper
            {'paperId': 'test1', 'title': 'Test', 'authors': []},  # Empty authors list
            {'paperId': 'test2', 'title': 'Test', 'citationCount': None},  # None values
        ]
        
        # Act - Should not crash
        results = process_papers(malformed_papers, "test")
        
        # Assert
        assert len(results) == 3
        # All should have been processed
        assert all('status' in r for r in results)
        assert all('paperId' in r for r in results)

    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_api_retry_mechanism_works(self, mock_sleep, mock_get):
        """Test that API retries work in the full pipeline context."""
        # Arrange - First call fails, second succeeds
        mock_get.side_effect = [
            Mock(status_code=500),  # First attempt fails
            Mock(status_code=200, json=lambda: {'data': [{'paperId': 'test', 'title': 'Test'}]})
        ]
        
        # Act
        papers = search_semantic_scholar("test", limit=1)
        
        # Assert
        assert len(papers) == 1
        assert mock_get.call_count == 2  # Retried once


# ============================================================================
# TEST CLASS: Integration with Real-World Patterns
# ============================================================================

class TestRealWorldPatterns:
    """Test patterns that reflect real-world usage."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.requests.get')
    @patch('src.DataPipeline.Ingestion.semantic_scholar_client.time.sleep')
    def test_typical_workflow_pattern(self, mock_api_sleep, mock_api_get,
                                      mock_extract_sleep, mock_process_sleep,
                                      complete_paper):
        """Test the typical user workflow pattern."""
        # Arrange
        search_term = "deep learning"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [complete_paper, complete_paper]}
        mock_api_get.return_value = mock_response
        
        # Act - Typical workflow
        # Step 1: Search for papers
        papers = search_semantic_scholar(search_term, limit=2)
        
        # Step 2: Verify we got results
        assert len(papers) > 0
        
        # Step 3: Process all papers
        results = process_papers(papers, search_term)
        
        # Step 4: Validate results
        assert len(results) == len(papers)
        
        # Step 5: Check success rate
        successful = [r for r in results if 'success' in r.get('status', '')]
        success_rate = len(successful) / len(results)
        
        # Assert - Should have high success rate with fallback
        assert success_rate > 0, "No papers successfully processed"

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.content_extractor.time.sleep')
    def test_metadata_preserved_through_pipeline(self, mock_extract_sleep, 
                                                 mock_process_sleep, complete_paper):
        """Test that metadata is preserved throughout the pipeline."""
        # Act
        results = process_papers([complete_paper], "test")
        result = results[0]
        
        # Assert - Original metadata preserved
        assert result['paperId'] == complete_paper['paperId']
        assert result['title'] == complete_paper['title']
        assert result['year'] == complete_paper['year']
        assert result['citationCount'] == complete_paper['citationCount']
        
        # New processing metadata added
        assert 'introduction' in result
        assert 'extraction_method' in result
        assert 'status' in result
        assert 'scraped_at' in result


# ============================================================================
# MARK TESTS AS INTEGRATION
# ============================================================================

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration