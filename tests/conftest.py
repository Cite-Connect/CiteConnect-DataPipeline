"""
Shared pytest fixtures and configuration for all tests.
This file is automatically discovered by pytest.
"""
import pytest
import pandas as pd
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock
from typing import Dict, List, Any

# ============================================================================
# DIRECTORY FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent

@pytest.fixture(scope="session")
def test_data_dir(project_root):
    """Return the test data directory."""
    test_dir = project_root / "tests" / "fixtures"
    test_dir.mkdir(parents=True, exist_ok=True)
    return test_dir

@pytest.fixture(scope="session")
def temp_output_dir(tmp_path_factory):
    """Create a temporary directory for test outputs."""
    return tmp_path_factory.mktemp("test_outputs")

# ============================================================================
# SAMPLE DATA FIXTURES
# ============================================================================

@pytest.fixture
def sample_paper_metadata():
    """Return sample paper metadata matching Semantic Scholar structure."""
    return {
        'paperId': '204e3073870fae3d05bcbc2f6a8e263d9b72e776',
        'title': 'Attention Is All You Need',
        'abstract': 'The dominant sequence transduction models are based on complex recurrent or convolutional neural networks...',
        'year': 2017,
        'publicationDate': '2017-06-12',
        'authors': [
            {'authorId': '1234', 'name': 'Ashish Vaswani'},
            {'authorId': '5678', 'name': 'Noam Shazeer'}
        ],
        'venue': 'NeurIPS',
        'citationCount': 89234,
        'influentialCitationCount': 12456,
        'referenceCount': 42,
        'isOpenAccess': True,
        'externalIds': {
            'ArXiv': '1706.03762',
            'DOI': '10.5555/3295222.3295349'
        },
        'fieldsOfStudy': ['Computer Science', 'Mathematics'],
        's2FieldsOfStudy': [
            {'category': 'Computer Science', 'source': 'external'},
            {'category': 'Machine Learning', 'source': 's2-fos-model'}
        ],
        'publicationTypes': ['JournalArticle'],
        'openAccessPdf': {
            'url': 'https://arxiv.org/pdf/1706.03762.pdf',
            'status': 'GREEN'
        },
        'tldr': {
            'model': 'tldr@v2.0.0',
            'text': 'The Transformer is the first transduction model relying entirely on self-attention.'
        },
        'citations': ['paper1', 'paper2'],
        'references': ['ref1', 'ref2']
    }

@pytest.fixture
def sample_paper_list():
    """Return a list of sample papers."""
    base = {
        'paperId': 'test123',
        'title': 'Test Paper',
        'abstract': 'This is a test abstract.',
        'year': 2020,
        'citationCount': 10
    }
    return [
        {**base, 'paperId': f'paper{i}', 'title': f'Test Paper {i}'}
        for i in range(5)
    ]

# ============================================================================
# API MOCK FIXTURES
# ============================================================================

@pytest.fixture
def mock_semantic_scholar_response():
    """Return a mock Semantic Scholar API response."""
    return {
        'total': 100,
        'offset': 0,
        'next': 10,
        'data': [
            {
                'paperId': 'abc123',
                'title': 'Sample Paper',
                'abstract': 'Sample abstract text.',
                'year': 2023
            }
        ]
    }

@pytest.fixture
def mock_failed_api_response():
    """Return a mock failed API response."""
    return Mock(status_code=500, text='Internal Server Error')

# ============================================================================
# CONFIGURATION FIXTURES
# ============================================================================

@pytest.fixture
def test_config():
    """Return test configuration dictionary."""
    return {
        'rate_limit_with_key': 1.5,
        'rate_limit_without_key': 5.0,
        'max_retries': 3,
        'timeout': 30,
        'fields': [
            'paperId', 'title', 'abstract', 'year',
            'authors', 'citationCount', 'venue'
        ]
    }

# ============================================================================
# ENVIRONMENT FIXTURES
# ============================================================================

@pytest.fixture
def mock_env_with_api_key(monkeypatch):
    """Set environment variable for API key."""
    monkeypatch.setenv('SEMANTIC_SCHOLAR_KEY', 'test-api-key-12345')
    return 'test-api-key-12345'

@pytest.fixture
def mock_env_without_api_key(monkeypatch):
    """Remove API key from environment."""
    monkeypatch.delenv('SEMANTIC_SCHOLAR_KEY', raising=False)

# ============================================================================
# DATA QUALITY FIXTURES
# ============================================================================

@pytest.fixture
def valid_dataframe():
    """Return a valid DataFrame with expected schema."""
    return pd.DataFrame({
        'paperId': ['paper1', 'paper2', 'paper3'],
        'title': ['Title 1', 'Title 2', 'Title 3'],
        'abstract': ['Abstract 1', 'Abstract 2', 'Abstract 3'],
        'year': [2020, 2021, 2022],
        'citationCount': [10, 20, 30],
        'extraction_method': ['arxiv_html', 'grobid_pdf', 'abstract_tldr'],
        'content_quality': ['high', 'high', 'low'],
        'has_intro': [True, True, False],
        'intro_length': [3000, 2500, 0]
    })

@pytest.fixture
def invalid_dataframe():
    """Return a DataFrame with data quality issues."""
    return pd.DataFrame({
        'paperId': ['paper1', None, 'paper3'],  # Missing ID
        'title': ['Title 1', 'Title 2', ''],  # Empty title
        'abstract': ['Abstract 1', 'Abstract 2', 'Abstract 3'],
        'year': [2020, 2050, 1900],  # Invalid years
        'citationCount': [10, -5, 30],  # Negative count
    })

# ============================================================================
# CLEANUP FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_test_files():
    """Automatically cleanup test files after each test."""
    yield
    # Cleanup logic here if needed
    pass
