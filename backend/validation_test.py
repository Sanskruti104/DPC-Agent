import sys
import os
import json
import uuid
import time
from typing import Any, Dict, List

# Add the project root to sys.path
sys.path.append(os.getcwd())

# Import the necessary modules
try:
    from app.rule_engine.orchestrator import run_compliance_check
    from app.violation_engine.orchestrator import run_violation_engine
    from app.explainability_engine.explainer import attach_explanations
    from app.reporting_engine.report_builder import build_report
    from app.schema_mapper.mapping_engine import map_fields, map_single_field
except ImportError as e:
    print(f"FAILED: Import error: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# 1. TEST INPUT DATA
# ---------------------------------------------------------

test_input = {
    "dataset": {
        "dataset_id": "test_validation",
        "columns": ["txn_value", "transaction_date", "status"],
        "rows": [
            {"txn_value": 500, "transaction_date": "2024-01-01", "status": "ACTIVE"},
            {"txn_value": 15000, "transaction_date": "2024-01-02", "status": "INACTIVE"},
            {"txn_value": None, "transaction_date": "2024-01-03", "status": "ACTIVE"}
        ]
    },
    "rules": [
        "amount > 1000",
        "amount < 20000"
    ],
    "mappings": [
        {
            "rule_field": "amount",
            "mapped_column": "txn_value",
            "confidence": 0.95,
            "match_type": "semantic"
        }
    ]
}

def validate():
    print("--- 🚀 Starting Full System Validation ---")

    # ---------------------------------------------------------
    # 2. Module 6: Schema Mapping Verification
    # ---------------------------------------------------------
    print("\n[Module 6] Verifying Schema Mapping...")
    rule_fields = ["amount"]
    dataset_cols = test_input["dataset"]["columns"]
    
    try:
        # Check mapping engine with debug info to see semantic scores
        mapping_result = map_single_field("amount", dataset_cols, include_debug=True)
        print(f"Mapping Result for 'amount': {mapping_result['mapped_column']} (Type: {mapping_result['match_type']}, Conf: {mapping_result['confidence']:.4f})")
        
        if mapping_result["debug"]:
            print("Top candidates:")
            for cand in mapping_result["debug"]["top_candidates"]:
                print(f"  - {cand['column']}: {cand['score']:.4f}")
        
        # Check requirements:
        if mapping_result["mapped_column"] == "txn_value":
            print("✅ rule_field correctly maps to txn_value")
        else:
            print(f"⚠️  ISSUE: Module 6 failed to map 'amount' to 'txn_value' automatically at default threshold.")
            
        if "confidence" in mapping_result and "match_type" in mapping_result:
            print("✅ Confidence and match_type are present")
    except Exception as e:
        print(f"❌ Module 6 failure: {e}")

    # ---------------------------------------------------------
    # 3. Module 4, 7, 8, 9: End-to-End Pipeline
    # -----------------------------------------
    print("\n[Modules 4, 7, 8, 9] Verifying End-to-End Pipeline...")
    
    try:
        # Step 4: Run Compliance Check
        execution_output = run_compliance_check(
            dataset=test_input["dataset"],
            rules_list=test_input["rules"],
            mappings=test_input["mappings"]
        )
        print("✅ Module 4: Compliance check executed.")
        
        # Step 7: Run Violation Engine
        enriched_violations = run_violation_engine(execution_output)
        print(f"✅ Module 7: Violation enrichment complete. Count: {len(enriched_violations)}")
        
        # Check Module 7 requirements
        violation_ids = set()
        for v in enriched_violations:
            required_fields = ["violation_id", "rule", "row_index", "column", "value", "expected", "message", "severity"]
            for f in required_fields:
                if not hasattr(v, f):
                    print(f"❌ Module 7 violation missing field: {f}")
            
            # Check for UUID uniqueness
            if v.violation_id in violation_ids:
                print(f"❌ Duplicate Violation ID found: {v.violation_id}")
            violation_ids.add(v.violation_id)
            
            # Check severity is valid
            if v.severity not in ["high", "medium", "low"]:
                print(f"⚠️ Unexpected severity level: {v.severity}")

        # Step 8: Run Explainability Engine
        explanations = attach_explanations(enriched_violations)
        print("✅ Module 8: Explanations attached.")
        
        for v in explanations:
            if not v.explanation:
                 print(f"❌ Empty explanation for violation of rule {v.rule}")
            if "{value}" in v.explanation or "{column}" in v.explanation:
                print(f"❌ Placeholder remaining in explanation: {v.explanation}")
        
        # Step 9: Run Reporting Engine
        final_report = build_report(execution_output["summary"], explanations)
        print("✅ Module 9: Final report built.")
        
        # Check Report structure
        required_report_fields = ["summary", "violations", "metrics", "chart_data"]
        for f in required_report_fields:
            if f not in final_report:
                print(f"❌ Module 9 report missing field: {f}")
        
        # Check data consistency
        total_violations_in_metrics = sum(final_report["metrics"].values())
        if total_violations_in_metrics == len(final_report["violations"]):
            print("✅ Metrics counts match violation count.")
        else:
            print(f"❌ Metric mismatch: {total_violations_in_metrics} vs {len(final_report['violations'])}")
            
        print(f"Report Summary: {final_report['summary']}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Pipeline failure: {e}")

    # ---------------------------------------------------------
    # 4. EDGE CASE TESTS
    # ---------------------------------------------------------
    print("\n[Edge Cases] Testing rule evaluation robustness...")
    edge_cases = [
        {"rule": "amount >= 1000", "dataset_vals": [1000, 500], "expected_violations": 1},
        {"rule": "status == ACTIVE", "dataset_vals": ["ACTIVE", "active"], "expected_violations": 1}, # Testing case sensitivity
        {"rule": "amount > 0", "dataset_vals": [None, 0, "string_val"], "expected_violations": 3}  # Testing type safety
    ]
    
    from app.rule_engine.rule_parser import parse_rule
    from app.rule_engine.execution_engine import execute_rule
    
    for ec in edge_cases:
        print(f"Testing Edge Case Rule: '{ec['rule']}' with vals {ec['dataset_vals']}")
        try:
            p_rule = parse_rule(ec['rule'])
            dataset_rows = [{"val": v} for v in ec['dataset_vals']]
            res = execute_rule(p_rule, "val", dataset_rows)
            v_count = len(res['violations'])
            print(f"  -> Result: {v_count} violations found.")
            if v_count != ec["expected_violations"]:
                print(f"  ⚠️  Unexpected violation count. Expected {ec['expected_violations']}, got {v_count}")
        except Exception as e:
            print(f"  ❌ Edge case failed with exception: {e}")

    # ---------------------------------------------------------
    # 5. PERFORMANCE CHECK
    # ---------------------------------------------------------
    print("\n[Performance] Simulating 500 rows...")
    large_rows = [{"txn_value": i, "transaction_date": "2024-01-01", "status": "ACTIVE"} for i in range(500)]
    large_dataset = {
        "dataset_id": "perf_test",
        "columns": ["txn_value", "transaction_date", "status"],
        "rows": large_rows
    }
    
    start_time = time.time()
    try:
        # Full pipeline test
        exec_out = run_compliance_check(large_dataset, test_input["rules"], test_input["mappings"])
        enriched = run_violation_engine(exec_out)
        explained = attach_explanations(enriched)
        report = build_report(exec_out["summary"], explained)
        end_time = time.time()
        print(f"Performance: Full pipeline for 500 rows in {end_time - start_time:.4f} seconds.")
    except Exception as e:
        print(f"❌ Performance test failed: {e}")

    print("\n--- ✅ System Validation Finished ---")

if __name__ == "__main__":
    validate()
