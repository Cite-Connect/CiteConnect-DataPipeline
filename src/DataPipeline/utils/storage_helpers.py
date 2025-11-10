# src/DataPipeline/utils/storage.py
"""
Minimal storage helper for local data
"""

import json
from datetime import datetime
from pathlib import Path


def save_data(data, filename=None):
    """Save data to JSON file."""
    # Create directory
    Path("./data/raw").mkdir(parents=True, exist_ok=True)
    
    # Generate filename if needed
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") 
        filename = f"data_{timestamp}.json"
    
    # Save file
    filepath = f"./data/raw/{filename}"
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"💾 Saved to: {filepath}")
    return filepath


def load_data(filename):
    """Load data from JSON file."""
    filepath = f"./data/raw/{filename}"
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print(f"📖 Loaded from: {filepath}")
    return data