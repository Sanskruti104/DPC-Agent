"""
orchestrator.py
---------------
Batch execution orchestrator linking the rule engine to the schema mapping 
and standardized datasets. 1:1 Correlation of rules to columns.
"""

from typing import List, Dict, Any, Optional
from app.rule_engine.rule_parser import parse_rule
from app.rule_engine.execution_engine import execute_rule
from app.rule_engine.summary import generate_summary

def run_compliance_check(
    dataset: Dict[str, Any], 
    rules_list: List[str], 
    mappings: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Orchestrates the multi-rule evaluation across a standardized dataset using 
    calculated mapping metadata.
    
    Args:
        dataset: Standardized dataset { "dataset_id", "columns", "rows" }
        rules_list: List of literal string rules (e.g., ["amount > 1000", "status == 'OK'"])
        mappings: Output from schema mapping engine (list of mapped column resolutions)
        
    Returns:
        Dict: Final composite evaluation report consisting of global summary and detailed rule reports.
    """
    dataset_rows = dataset.get("rows", [])
    total_rows = len(dataset_rows)
    
    # Use a lookup structure for the mapping engine output for O(1) correlation
    mapping_lookup = { m["rule_field"]: m["mapped_column"] for m in mappings }
    
    rule_results = []
    total_violations_across_rules = 0
    unique_violated_rows = set() # Track unique row indices to properly calculate overall compliance

    # 1. Loop through each rule for execution
    for rule_str in rules_list:
        # 2. Parse the rule to extract the abstract field name
        parsed_rule = parse_rule(rule_str)
        rule_field = parsed_rule.get("field")
        
        # 3. Find the Mapped Column in the dataset schema
        mapped_column = mapping_lookup.get(rule_field)
        
        # 4. Skip execution if No Column is mapped for this rule (Consider standard policy behavior)
        if not mapped_column:
             rule_results.append({
                 "rule": rule_str,
                 "mapped_column": None,
                 "status": "skipped",
                 "reason": "Missing column mapping in dataset"
             })
             continue
        
        # 5. Execute rule logic across the dataset
        result = execute_rule(parsed_rule, mapped_column, dataset_rows)
        
        # 6. Accumulate global violation metrics for the summary
        rule_results.append(result)
        
        # Track which rows violated *this* specific rule
        for violation in result.get("violations", []):
            unique_violated_rows.add(violation.get("row_index"))

    # 7. Generate consolidated aggregate metrics
    # Note: Compliance rate is calculated based on how many rows failed ANY rule check.
    summary = generate_summary(total_rows, len(unique_violated_rows))
    
    return {
        "summary": summary,
        "rule_results": rule_results
    }
