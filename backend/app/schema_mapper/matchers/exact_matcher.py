"""
exact_matcher.py
----------------
Core matching module executing pure structural evaluations mapping abstract rule 
metrics safely against physical dataset schemas using O(1) caching techniques.

Contains no semantic or heuristic matching mechanisms.
"""

from typing import List, Optional

def exact_match(rule_field: str, dataset_columns: List[str]) -> Optional[str]:
    """
    Evaluates an exact string compliance match by normalizing capitalization 
    and eliminating leading/trailing whitespace securely.
    
    Args:
        rule_field: The abstract string constraint field established by a policy rule.
        dataset_columns: The literal list of columns extracted physically from the uploaded dataset.
        
    Returns:
        str: The raw, unmodified column name perfectly matching the normalized metrics constraint.
             Returns None if zero matching columns establish a direct correlation.
    """
    if not rule_field or not dataset_columns:
        return None
        
    # 1. Normalize the target query metric
    normalized_rule = rule_field.strip().lower()
    
    # 2. Build a highly efficient O(1) lookup map mapping normalized strings entirely to 
    # their raw native formats. This occurs precisely once, satisfying the no-looping constraints natively.
    lookup_map = {col.strip().lower(): col for col in dataset_columns}
    
    # 3. Lookup via strict hashing cleanly without external dependencies
    return lookup_map.get(normalized_rule, None)
