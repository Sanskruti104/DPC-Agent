"""
summary.py
----------
Utility module for summarizing overall compliance metrics and calculating 
the total health of the analyzed data.
"""

from typing import Dict, Any

def generate_summary(total_rows: int, total_violations: int) -> Dict[str, Any]:
    """
    Generates a high-level summary of policy compliance across the dataset.
    
    Args:
        total_rows: The total number of rows analyzed.
        total_violations: The number of violations (rows identifying as False under a rule).
        
    Returns:
        Dict: Final summary Metrics containing 'total_rows', 'violations', and 'compliance_rate'.
    """
    # 1. Calculation - handle edge case of zero rows (no compliance rate)
    if total_rows <= 0:
        return {
            "total_rows": 0,
            "violations": 0,
            "compliance_rate": 0.0
        }
        
    # 2. Formula (from user instructions)
    compliance_rate = ((total_rows - total_violations) / total_rows) * 100
    
    # 3. Final Output structure
    return {
        "total_rows": total_rows,
        "violations": total_violations,
        "compliance_rate": round(compliance_rate, 2)
    }
