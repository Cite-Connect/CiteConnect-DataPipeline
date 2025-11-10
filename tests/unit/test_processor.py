"""
Unit tests for processor.py

Tests cover:
- Paper processing orchestration
- Integration between metadata extraction and content extraction
- Success and failure scenarios
- Record updates based on extraction results
- Batch processing of multiple papers
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import time

from src.DataPipeline.Ingestion.processor import process_papers


class TestProcessPapersBasic:
    """Test basic paper processing functionality."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_processes_single_paper(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test processing of a single paper."""
        # Arrange
        papers = [{'paperId': 'test123', 'title': 'Test Paper'}]
        
        # Mock metadata extraction
        mock_extract_metadata.return_value = {
            'paperId': 'test123',
            'title': 'Test Paper',
            'introduction': None,
            'extraction_method': None,
            'content_quality': None,
            'has_intro': False,
            'intro_length': 0,
            'status': 'pending'
        }
        
        # Mock content extraction
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Intro text', 'arxiv_html', 'high')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "machine learning")
        
        # Assert
        assert len(results) == 1
        assert results[0]['paperId'] == 'test123'
        assert results[0]['introduction'] == 'Intro text'
        assert results[0]['extraction_method'] == 'arxiv_html'
        assert results[0]['content_quality'] == 'high'
        assert results[0]['has_intro'] is True
        assert results[0]['intro_length'] == 10  # len('Intro text')
        assert results[0]['status'] == 'success_arxiv_html'

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_processes_multiple_papers(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test processing of multiple papers."""
        # Arrange
        papers = [
            {'paperId': 'paper1', 'title': 'Paper 1'},
            {'paperId': 'paper2', 'title': 'Paper 2'},
            {'paperId': 'paper3', 'title': 'Paper 3'}
        ]
        
        # Mock metadata extraction to return unique records
        def metadata_side_effect(paper, search_term):
            return {
                'paperId': paper['paperId'],
                'title': paper['title'],
                'introduction': None,
                'status': 'pending'
            }
        mock_extract_metadata.side_effect = metadata_side_effect
        
        # Mock content extraction
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'grobid_pdf', 'high')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test query")
        
        # Assert
        assert len(results) == 3
        assert results[0]['paperId'] == 'paper1'
        assert results[1]['paperId'] == 'paper2'
        assert results[2]['paperId'] == 'paper3'
        assert all(r['introduction'] == 'Content' for r in results)

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_returns_empty_list_for_empty_input(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that empty input returns empty list."""
        # Arrange
        papers = []
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results == []
        assert isinstance(results, list)


class TestProcessPapersContentExtraction:
    """Test content extraction integration."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_updates_record_on_successful_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that record is updated when content extraction succeeds."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'introduction': None,
            'extraction_method': None,
            'content_quality': None,
            'has_intro': False,
            'intro_length': 0,
            'status': 'pending'
        }
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = (
            'Extracted introduction text',
            'regex_pdf',
            'medium'
        )
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['introduction'] == 'Extracted introduction text'
        assert results[0]['extraction_method'] == 'regex_pdf'
        assert results[0]['content_quality'] == 'medium'
        assert results[0]['has_intro'] is True
        assert results[0]['intro_length'] == 27
        assert results[0]['status'] == 'success_regex_pdf'

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_handles_failed_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that failed extraction sets fail_reason."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'introduction': None,
            'status': 'pending'
        }
        
        # Mock content extraction failure (returns None for content)
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = (None, 'failed', 'none')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['introduction'] is None
        assert results[0]['fail_reason'] == 'extraction_failed'
        assert 'has_intro' not in results[0] or results[0].get('has_intro') is None

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_handles_empty_content_string(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that empty string content is treated as failure."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'introduction': None,
            'status': 'pending'
        }
        
        # Mock empty content string
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        # Empty string is falsy in Python, so should be treated as failure
        assert results[0]['fail_reason'] == 'extraction_failed'


class TestProcessPapersExtractionMethods:
    """Test different extraction methods."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_arxiv_html_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test ArXiv HTML extraction method."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'status': 'pending'
        }
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'arxiv_html', 'high')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['extraction_method'] == 'arxiv_html'
        assert results[0]['content_quality'] == 'high'
        assert results[0]['status'] == 'success_arxiv_html'

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_grobid_pdf_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test GROBID PDF extraction method."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'status': 'pending'
        }
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'grobid_pdf', 'high')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['extraction_method'] == 'grobid_pdf'
        assert results[0]['status'] == 'success_grobid_pdf'

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_regex_pdf_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test regex PDF extraction method."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'status': 'pending'
        }
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'regex_pdf', 'medium')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['extraction_method'] == 'regex_pdf'
        assert results[0]['content_quality'] == 'medium'
        assert results[0]['status'] == 'success_regex_pdf'

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_abstract_tldr_fallback(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test abstract+TLDR fallback method."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {
            'paperId': 'test',
            'status': 'pending'
        }
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Abstract content', 'abstract_tldr', 'low')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['extraction_method'] == 'abstract_tldr'
        assert results[0]['content_quality'] == 'low'
        assert results[0]['status'] == 'success_abstract_tldr'


class TestProcessPapersRateLimiting:
    """Test rate limiting between papers."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_sleeps_between_papers(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that sleep is called between processing papers."""
        # Arrange
        papers = [
            {'paperId': 'paper1', 'title': 'Paper 1'},
            {'paperId': 'paper2', 'title': 'Paper 2'},
            {'paperId': 'paper3', 'title': 'Paper 3'}
        ]
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        process_papers(papers, "test")
        
        # Assert
        assert mock_sleep.call_count == 3  # Once per paper
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 1  # Sleep for 1 second

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_sleep_duration_is_one_second(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that sleep duration is 1 second."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        process_papers(papers, "test")
        
        # Assert
        mock_sleep.assert_called_with(1)


class TestProcessPapersMixedResults:
    """Test processing with mixed success/failure scenarios."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_handles_mixed_success_and_failure(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test processing papers with some successes and some failures."""
        # Arrange
        papers = [
            {'paperId': 'success1', 'title': 'Success 1'},
            {'paperId': 'fail1', 'title': 'Fail 1'},
            {'paperId': 'success2', 'title': 'Success 2'}
        ]
        
        def metadata_side_effect(paper, search_term):
            return {'paperId': paper['paperId'], 'status': 'pending'}
        mock_extract_metadata.side_effect = metadata_side_effect
        
        # Mock mixed extraction results
        mock_extractor = Mock()
        mock_extractor.extract_content.side_effect = [
            ('Content 1', 'arxiv_html', 'high'),   # Success
            (None, 'failed', 'none'),               # Failure
            ('Content 2', 'grobid_pdf', 'high')    # Success
        ]
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert len(results) == 3
        assert results[0]['introduction'] == 'Content 1'
        assert results[0]['status'] == 'success_arxiv_html'
        assert results[1]['fail_reason'] == 'extraction_failed'
        assert results[2]['introduction'] == 'Content 2'
        assert results[2]['status'] == 'success_grobid_pdf'


class TestProcessPapersLogging:
    """Test logging functionality."""

    @patch('src.DataPipeline.Ingestion.processor.logging')
    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_logs_paper_processing(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep, mock_logging):
        """Test that paper processing is logged."""
        # Arrange
        papers = [
            {'paperId': 'test1', 'title': 'Test Paper Title'},
            {'paperId': 'test2', 'title': 'Another Paper'}
        ]
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        process_papers(papers, "test")
        
        # Assert
        assert mock_logging.info.call_count >= 2  # At least once per paper

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_handles_missing_title_in_logging(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that missing title doesn't crash logging."""
        # Arrange
        papers = [{'paperId': 'test'}]  # No title
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act & Assert - should not raise exception
        results = process_papers(papers, "test")
        assert len(results) == 1


class TestProcessPapersEdgeCases:
    """Test edge cases and unusual scenarios."""

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_handles_very_long_content(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test handling of very long extracted content."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        long_content = 'A' * 50000  # Very long content
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = (long_content, 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        results = process_papers(papers, "test")
        
        # Assert
        assert results[0]['introduction'] == long_content
        assert results[0]['intro_length'] == 50000

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_passes_search_term_to_metadata_extraction(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that search term is passed to metadata extraction."""
        # Arrange
        papers = [{'paperId': 'test', 'title': 'Test'}]
        search_term = "deep learning"
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        process_papers(papers, search_term)
        
        # Assert
        # Verify extract_metadata was called with correct search_term
        for call in mock_extract_metadata.call_args_list:
            assert call[0][1] == search_term  # Second argument is search_term

    @patch('src.DataPipeline.Ingestion.processor.time.sleep')
    @patch('src.DataPipeline.Ingestion.processor.ContentExtractor')
    @patch('src.DataPipeline.Ingestion.processor.extract_metadata')
    def test_creates_one_extractor_instance(self, mock_extract_metadata, mock_content_extractor_class, mock_sleep):
        """Test that only one ContentExtractor instance is created."""
        # Arrange
        papers = [
            {'paperId': 'test1', 'title': 'Test 1'},
            {'paperId': 'test2', 'title': 'Test 2'}
        ]
        
        mock_extract_metadata.return_value = {'paperId': 'test', 'status': 'pending'}
        
        mock_extractor = Mock()
        mock_extractor.extract_content.return_value = ('Content', 'method', 'quality')
        mock_content_extractor_class.return_value = mock_extractor
        
        # Act
        process_papers(papers, "test")
        
        # Assert
        # ContentExtractor should be instantiated only once
        assert mock_content_extractor_class.call_count == 1