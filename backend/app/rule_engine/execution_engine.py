"""
execution_engine.py
-------------------
The core execution logic responsible for evaluating a single policy rule 
across an entire standardized dataset array.
"""

from typing import Any, Dict, List
from app.rule_engine.row_evaluator import evaluate_row

def execute_rule(parsed_rule: Dict[str, Any], mapped_column: str, dataset_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Evaluates one policy rule sequentially across all dataset records.
    
    Args:
        parsed_rule: The output of the rule_parser.
        mapped_column: The literal string identifier of the dataset column.
        dataset_rows: A list of standardized dictionaries representing the dataset (from standardization service).
        
    Returns:
        Dict: Complete evaluation report for this rule across the dataset.
    """
    total_rows = len(dataset_rows)
    violations = []
    
    # 1. Loop through all rows for evaluation
    for index, row in enumerate(dataset_rows):
        # 2. Use row_evaluator for single-record assessment
        result = evaluate_row(parsed_rule, mapped_column, row)
        
        # 3. Collect violations where the rule evaluation returned False
        if not result.get("passed", True):
            violations.append({
                "row_index": index,
                "value": result.get("value")
            })
            
    # 4. Consolidate results into finalized report
    return {
        "rule": f"{parsed_rule.get('field')} {parsed_rule.get('operator')} {parsed_rule.get('value')}",
        "mapped_column": mapped_column,
        "total_rows": total_rows,
        "violations": violations
    }
