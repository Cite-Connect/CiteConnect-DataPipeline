"""
Unit tests for metadata_utils.py

Tests cover:
- Metadata field extraction from paper objects
- Handling of missing/null values
- Data type conversions (JSON serialization)
- Author name parsing and joining
- Safe nested dictionary access
- Edge cases and malformed data
"""
import pytest
import json
from datetime import datetime
from unittest.mock import patch

from src.DataPipeline.Ingestion.metadata_utils import extract_metadata


class TestExtractMetadataBasic:
    """Test basic metadata extraction with complete data."""

    def test_extracts_all_required_fields(self):
        """Test that all expected fields are present in output."""
        # Arrange
        paper = {
            'paperId': 'abc123',
            'title': 'Test Paper',
            'abstract': 'Test abstract',
            'year': 2023
        }
        
        # Act
        result = extract_metadata(paper, "machine learning")
        
        # Assert - verify all expected fields exist
        expected_fields = [
            'search_term', 'paperId', 'externalIds', 'title', 'abstract',
            'year', 'publicationDate', 'authors', 'citationCount',
            'referenceCount', 'fieldsOfStudy', 'pdf_url', 'tldr',
            'introduction', 'extraction_method', 'content_quality',
            'has_intro', 'intro_length', 'status', 'scraped_at'
        ]
        for field in expected_fields:
            assert field in result, f"Missing field: {field}"

    def test_extracts_paper_id(self):
        """Test paperId extraction."""
        # Arrange
        paper = {'paperId': 'test123'}
        
        # Act
        result = extract_metadata(paper, "test query")
        
        # Assert
        assert result['paperId'] == 'test123'

    def test_extracts_title(self):
        """Test title extraction."""
        # Arrange
        paper = {'title': 'Attention Is All You Need'}
        
        # Act
        result = extract_metadata(paper, "transformers")
        
        # Assert
        assert result['title'] == 'Attention Is All You Need'

    def test_extracts_abstract(self):
        """Test abstract extraction."""
        # Arrange
        paper = {'abstract': 'This is the abstract text.'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['abstract'] == 'This is the abstract text.'

    def test_extracts_year(self):
        """Test year extraction."""
        # Arrange
        paper = {'year': 2020}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['year'] == 2020

    def test_extracts_publication_date(self):
        """Test publicationDate extraction."""
        # Arrange
        paper = {'publicationDate': '2020-06-15'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['publicationDate'] == '2020-06-15'

    def test_stores_search_term(self):
        """Test that search term is stored correctly."""
        # Arrange
        paper = {'paperId': 'test'}
        search_term = "deep learning"
        
        # Act
        result = extract_metadata(paper, search_term)
        
        # Assert
        assert result['search_term'] == "deep learning"


class TestExtractMetadataAuthors:
    """Test author name extraction and formatting."""

    def test_extracts_single_author(self):
        """Test extraction of single author name."""
        # Arrange
        paper = {
            'authors': [
                {'name': 'John Doe'}
            ]
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == 'John Doe'

    def test_extracts_multiple_authors(self):
        """Test extraction and joining of multiple authors."""
        # Arrange
        paper = {
            'authors': [
                {'name': 'John Doe'},
                {'name': 'Jane Smith'},
                {'name': 'Bob Johnson'}
            ]
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == 'John Doe, Jane Smith, Bob Johnson'

    def test_handles_missing_authors_field(self):
        """Test that missing authors field returns empty string."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == ''

    def test_handles_empty_authors_list(self):
        """Test that empty authors list returns empty string."""
        # Arrange
        paper = {'authors': []}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == ''

    def test_handles_author_without_name(self):
        """Test that author without name field is handled gracefully."""
        # Arrange
        paper = {
            'authors': [
                {'authorId': '12345'},  # Missing 'name' field
                {'name': 'John Doe'}
            ]
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == ', John Doe'  # First author contributes empty string


class TestExtractMetadataCitationCounts:
    """Test citation and reference count extraction."""

    def test_extracts_citation_count(self):
        """Test citationCount extraction."""
        # Arrange
        paper = {'citationCount': 150}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['citationCount'] == 150

    def test_citation_count_defaults_to_zero(self):
        """Test that missing citationCount defaults to 0."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['citationCount'] == 0

    def test_extracts_reference_count(self):
        """Test referenceCount extraction."""
        # Arrange
        paper = {'referenceCount': 42}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['referenceCount'] == 42

    def test_reference_count_defaults_to_zero(self):
        """Test that missing referenceCount defaults to 0."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['referenceCount'] == 0


class TestExtractMetadataJSONFields:
    """Test JSON serialization of complex fields."""

    def test_serializes_external_ids(self):
        """Test externalIds are serialized to JSON string."""
        # Arrange
        paper = {
            'externalIds': {
                'ArXiv': '1706.03762',
                'DOI': '10.5555/3295222'
            }
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert isinstance(result['externalIds'], str)
        parsed = json.loads(result['externalIds'])
        assert parsed['ArXiv'] == '1706.03762'
        assert parsed['DOI'] == '10.5555/3295222'

    def test_external_ids_defaults_to_empty_json_object(self):
        """Test that missing externalIds becomes empty JSON object."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['externalIds'] == '{}'

    def test_serializes_fields_of_study(self):
        """Test fieldsOfStudy are serialized to JSON string."""
        # Arrange
        paper = {
            'fieldsOfStudy': ['Computer Science', 'Mathematics']
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert isinstance(result['fieldsOfStudy'], str)
        parsed = json.loads(result['fieldsOfStudy'])
        assert parsed == ['Computer Science', 'Mathematics']

    def test_fields_of_study_defaults_to_empty_json_array(self):
        """Test that missing fieldsOfStudy becomes empty JSON array."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['fieldsOfStudy'] == '[]'


class TestExtractMetadataSafeGet:
    """Test safe nested dictionary access."""

    def test_extracts_nested_pdf_url(self):
        """Test extraction of nested PDF URL."""
        # Arrange
        paper = {
            'openAccessPdf': {
                'url': 'https://example.com/paper.pdf'
            }
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['pdf_url'] == 'https://example.com/paper.pdf'

    def test_pdf_url_returns_none_when_missing(self):
        """Test that missing PDF URL returns None."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['pdf_url'] is None

    def test_pdf_url_returns_none_when_openAccessPdf_is_none(self):
        """Test that None openAccessPdf is handled."""
        # Arrange
        paper = {'openAccessPdf': None}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['pdf_url'] is None

    def test_extracts_nested_tldr_text(self):
        """Test extraction of nested TLDR text."""
        # Arrange
        paper = {
            'tldr': {
                'text': 'This is a summary.'
            }
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['tldr'] == 'This is a summary.'

    def test_tldr_returns_none_when_missing(self):
        """Test that missing TLDR returns None."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['tldr'] is None

    def test_tldr_returns_none_when_tldr_is_none(self):
        """Test that None TLDR is handled."""
        # Arrange
        paper = {'tldr': None}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['tldr'] is None


class TestExtractMetadataDefaultFields:
    """Test default values for content extraction fields."""

    def test_introduction_defaults_to_none(self):
        """Test that introduction field defaults to None."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['introduction'] is None

    def test_extraction_method_defaults_to_none(self):
        """Test that extraction_method defaults to None."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['extraction_method'] is None

    def test_content_quality_defaults_to_none(self):
        """Test that content_quality defaults to None."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['content_quality'] is None

    def test_has_intro_defaults_to_false(self):
        """Test that has_intro defaults to False."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['has_intro'] is False

    def test_intro_length_defaults_to_zero(self):
        """Test that intro_length defaults to 0."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['intro_length'] == 0

    def test_status_defaults_to_pending(self):
        """Test that status defaults to 'pending'."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['status'] == 'pending'


class TestExtractMetadataTimestamp:
    """Test timestamp generation."""

    @patch('src.DataPipeline.Ingestion.metadata_utils.datetime')
    def test_scraped_at_timestamp_format(self, mock_datetime):
        """Test that scraped_at has correct ISO format with Z suffix."""
        # Arrange
        mock_datetime.utcnow.return_value = datetime(2023, 6, 15, 10, 30, 45)
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['scraped_at'] == '2023-06-15T10:30:45Z'

    def test_scraped_at_is_string(self):
        """Test that scraped_at is a string."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert isinstance(result['scraped_at'], str)

    def test_scraped_at_ends_with_z(self):
        """Test that scraped_at timestamp ends with Z."""
        # Arrange
        paper = {'paperId': 'test'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['scraped_at'].endswith('Z')


class TestExtractMetadataEdgeCases:
    """Test edge cases and malformed data."""

    def test_handles_empty_paper_object(self):
        """Test that empty paper object doesn't crash."""
        # Arrange
        paper = {}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result is not None
        assert result['paperId'] is None
        assert result['citationCount'] == 0

    def test_handles_none_values_in_paper(self):
        """Test that None values are handled gracefully."""
        # Arrange
        paper = {
            'paperId': None,
            'title': None,
            'abstract': None,
            'year': None
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['paperId'] is None
        assert result['title'] is None
        assert result['abstract'] is None
        assert result['year'] is None

    def test_handles_zero_citation_count(self):
        """Test that zero citation count is preserved."""
        # Arrange
        paper = {'citationCount': 0}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['citationCount'] == 0

    def test_handles_negative_citation_count(self):
        """Test that negative citation count is preserved (edge case)."""
        # Arrange
        paper = {'citationCount': -1}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['citationCount'] == -1

    def test_handles_special_characters_in_title(self):
        """Test that special characters in title are preserved."""
        # Arrange
        paper = {'title': 'Test: Paper & Results (2023) - Part 1'}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['title'] == 'Test: Paper & Results (2023) - Part 1'

    def test_handles_unicode_in_author_names(self):
        """Test that unicode characters in author names are handled."""
        # Arrange
        paper = {
            'authors': [
                {'name': 'José García'},
                {'name': '李明'}
            ]
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['authors'] == 'José García, 李明'

    def test_handles_very_long_abstract(self):
        """Test that very long abstracts are handled."""
        # Arrange
        long_abstract = "A" * 10000
        paper = {'abstract': long_abstract}
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['abstract'] == long_abstract
        assert len(result['abstract']) == 10000


class TestExtractMetadataIntegration:
    """Test with realistic complete paper objects."""

    def test_realistic_paper_structure(self):
        """Test extraction from realistic Semantic Scholar paper."""
        # Arrange
        paper = {
            'paperId': '204e3073870fae3d05bcbc2f6a8e263d9b72e776',
            'externalIds': {
                'ArXiv': '1706.03762',
                'DOI': '10.5555/3295222.3295349'
            },
            'title': 'Attention Is All You Need',
            'abstract': 'The dominant sequence transduction models...',
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
                'url': 'https://arxiv.org/pdf/1706.03762.pdf',
                'status': 'GREEN'
            },
            'tldr': {
                'model': 'tldr@v2.0.0',
                'text': 'The Transformer model.'
            }
        }
        
        # Act
        result = extract_metadata(paper, "transformers")
        
        # Assert
        assert result['paperId'] == '204e3073870fae3d05bcbc2f6a8e263d9b72e776'
        assert result['title'] == 'Attention Is All You Need'
        assert result['year'] == 2017
        assert result['authors'] == 'Ashish Vaswani, Noam Shazeer'
        assert result['citationCount'] == 89234
        assert result['pdf_url'] == 'https://arxiv.org/pdf/1706.03762.pdf'
        assert result['tldr'] == 'The Transformer model.'
        assert 'ArXiv' in result['externalIds']
        assert 'Computer Science' in result['fieldsOfStudy']

    def test_minimal_paper_structure(self):
        """Test extraction from minimal paper with only required fields."""
        # Arrange
        paper = {
            'paperId': 'min123',
            'title': 'Minimal Paper'
        }
        
        # Act
        result = extract_metadata(paper, "test")
        
        # Assert
        assert result['paperId'] == 'min123'
        assert result['title'] == 'Minimal Paper'
        assert result['abstract'] is None
        assert result['year'] is None
        assert result['authors'] == ''
        assert result['citationCount'] == 0