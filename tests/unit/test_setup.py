"""
Sanity tests to verify testing environment is set up correctly.
"""
import pytest
import pandas as pd
import sys
from pathlib import Path

def test_python_version():
    """Verify Python version is 3.8+."""
    assert sys.version_info >= (3, 8), "Python 3.8+ required"

def test_pytest_working():
    """Verify pytest is working."""
    assert True

def test_pandas_import():
    """Verify pandas is available."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    assert len(df) == 3

def test_fixtures_directory(test_data_dir):
    """Verify test fixtures directory exists."""
    assert test_data_dir.exists()
    assert test_data_dir.is_dir()

def test_sample_paper_fixture(sample_paper_metadata):
    """Verify sample paper fixture works."""
    assert 'paperId' in sample_paper_metadata
    assert 'title' in sample_paper_metadata
    assert sample_paper_metadata['year'] == 2017

@pytest.mark.parametrize("value,expected", [
    (1, True),
    (0, False),
    (-1, False),
])
def test_parametrized_example(value, expected):
    """Example of parametrized testing."""
    assert (value > 0) == expected