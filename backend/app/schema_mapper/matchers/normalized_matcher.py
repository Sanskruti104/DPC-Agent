"""
normalized_matcher.py
---------------------
Normalization-based matching module for schema mapping.
Performs exact matching after stripping special characters, casing, and whitespace.
"""

import re
from typing import List, Optional

def normalize_text(text: str) -> str:
    """
    Normalizes text by:
    - Converting to lowercase
    - Removing all non-alphanumeric characters (spaces, underscores, hyphens, etc.)
    
    Args:
        text: The input string to normalize.
        
    Returns:
        str: The normalized alphanumeric string.
    """
    if not text:
        return ""
    # Remove all non-alphanumeric characters and convert to lowercase
    return re.sub(r'[^a-zA-Z0-9]', '', text).lower()

def normalized_match(rule_field: str, dataset_columns: List[str]) -> Optional[str]:
    """
    Attempts to match a rule field to a dataset column after normalization.
    
    Args:
        rule_field: The abstract rule field name.
        dataset_columns: List of actual column names from the dataset.
        
    Returns:
        Optional[str]: The original column name if a match is found, otherwise None.
    """
    if not rule_field or not dataset_columns:
        return None
        
    # Normalize the target rule field
    normalized_target = normalize_text(rule_field)
    
    # Build a lookup map of {normalized_name: original_name}
    # This is O(N) once to build, then O(1) for lookups.
    lookup_map = {normalize_text(col): col for col in dataset_columns}
    
    return lookup_map.get(normalized_target)
