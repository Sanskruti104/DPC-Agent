import sys
import os

# Ensure backend folder is in path
sys.path.insert(0, r'c:\Users\a\Desktop\Desktop Files\Hackathon\CODE_APEX\Vikrant\Policy-Compliance-Agent\Policy-Compliance-Agent\backend')

from app.schema_mapper.mapping_engine import map_fields

def test_batch_mapping():
    columns = ["txn_id", "Amount", "date", "full_name", "Transaction_Status"]
    rule_fields = ["Amount", "txnid", "transaction day", "weather"]
    
    print("--- Batch Hybrid Mapping Engine Tests ---")
    results = map_fields(columns, rule_fields)
    
    for res in results:
        print(f"Rule: {res['rule_field']:<15} | Match: {str(res['mapped_column']):<15} | Conf: {res['confidence']:.2f} | Type: {res['match_type']}")

if __name__ == "__main__":
    test_batch_mapping()
