"""
row_evaluator.py
----------------
System for evaluating a single dataset row against a single parsed policy rule.
Automatically correlates the abstract rule field to the concrete mapped column.
"""

from typing import Any, Dict
from app.rule_engine.operator_engine import evaluate

def evaluate_row(parsed_rule: Dict[str, Any], mapped_column: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates a specific rule against a single row using its mapped database column identifier.
    
    Args:
        parsed_rule: The output of the rule_parser (e.g., {"field": "amt", "operator": ">", "value": 100})
        mapped_column: The literal string name of the column found in the dataset rows.
        row: A single dictionary representing a dataset record (e.g., {"txn_amt": 500, "user": "A"})
        
    Returns:
        Dict: Final row assessment structure containing 'passed' and 'value'.
    """
    # 1. Capture the actual value from the row based on the mapped column ID
    # Use .get() to handle missing columns gracefully (returns None)
    actual_value = row.get(mapped_column)
    
    # 2. Extract components from the parsed rule
    op = parsed_rule.get("operator", "==")
    rule_value = parsed_rule.get("value")
    
    # 3. Handle Null/Missing value logic (a missing value generally fails strict comparisons)
    if actual_value is None:
        return {
            "passed": False,
            "value": None
        }
        
    # 4. Perform comparison
    passed = evaluate(op, actual_value, rule_value)
    
    return {
        "passed": passed,
        "value": actual_value
    }
