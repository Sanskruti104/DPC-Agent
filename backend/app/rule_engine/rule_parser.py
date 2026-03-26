"""
rule_parser.py
--------------
Module for parsing string-based policy rules into structured condition objects.
Example: "amount > 1000" -> {"field": "amount", "operator": ">", "value": 1000}
"""

import re
from typing import Any, Dict, Optional

# Supported operator list
OPERATORS = [">=", "<=", ">", "<", "==", "!="]

def parse_rule(rule_str: str) -> Dict[str, Any]:
    """
    Parses a rule string into its constituent components.
    
    Args:
        rule_str: The raw rule string (e.g., "age >= 18").
        
    Returns:
        Dict: Structured rule containing 'field', 'operator', and 'value'.
        
    Raises:
        ValueError: If the rule format is invalid or includes an unsupported operator.
    """
    rule_str = rule_str.strip()
    
    # 1. Identify the operator in the string
    # Sort operators by length descending to match multi-char operators (>=) before single-char (>)
    found_op = None
    for op in sorted(OPERATORS, key=len, reverse=True):
        if op in rule_str:
            found_op = op
            break
            
    if not found_op:
        raise ValueError(f"Invalid rule format: No supported operator found in '{rule_str}'")
        
    # 2. Split into field and value
    parts = rule_str.split(found_op)
    if len(parts) != 2:
        raise ValueError(f"Invalid rule format: Expected 'field operator value' in '{rule_str}'")
        
    field = parts[0].strip()
    raw_value = parts[1].strip()
    
    # 3. Handle value type conversion (Basic numeric inference)
    # Attempt to convert to float/int if possible, else keep as string
    parsed_value: Any = raw_value
    
    # Check for quotes (indicating string literal)
    if (raw_value.startswith("'") and raw_value.endswith("'")) or \
       (raw_value.startswith('"') and raw_value.endswith('"')):
        parsed_value = raw_value[1:-1]
    else:
        # Check if numeric
        try:
            if "." in raw_value:
                parsed_value = float(raw_value)
            else:
                parsed_value = int(raw_value)
        except ValueError:
            # Fallback to string if not numeric
            parsed_value = raw_value
            
    return {
        "field": field,
        "operator": found_op,
        "value": parsed_value
    }
