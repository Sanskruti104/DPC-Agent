"""
operator_engine.py
------------------
Dynamic evaluation module to apply comparison operators between data points.
"""

from typing import Any, Callable, Dict

# Mapping of operator strings to their respective lambda functions
OPERATOR_MAP: Dict[str, Callable[[Any, Any], bool]] = {
    ">":  lambda l, r: l > r,
    "<":  lambda l, r: l < r,
    ">=": lambda l, r: l >= r,
    "<=": lambda l, r: l <= r,
    "==": lambda l, r: l == r,
    "!=": lambda l, r: l != r,
}

def evaluate(operator: str, left: Any, right: Any) -> bool:
    """
    Evaluates a single comparison between a left and right value based on the operator.
    
    Args:
        operator: The comparison string (e.g., '>', '==').
        left: The value extracted from the dataset row.
        right: The literal value provided in the rule definition.
        
    Returns:
        bool: True if the condition is met, otherwise False.
        Returns False on invalid comparisons or mismatched types that crash Python.
    """
    if operator not in OPERATOR_MAP:
        return False
        
    try:
        # Perform comparison using the mapped lambda
        return OPERATOR_MAP[operator](left, right)
    except (TypeError, ValueError):
        # Gracefully handle comparison errors (e.g., comparing string to int) 
        # as a compliance violation/failure by returning False.
        return False
