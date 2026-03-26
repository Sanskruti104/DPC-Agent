"""
compliance_routes.py
--------------------
REST API endpoints for cross-referencing datasets against dynamic rules 
to evaluate data policy compliance metrics.
"""

from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.rule_engine.orchestrator import run_compliance_check

router = APIRouter()

# ---------------------------------------------------------------------------
# Contract Definition
# ---------------------------------------------------------------------------

class DatasetPayloadSchema(BaseModel):
    dataset_id: str
    columns: List[str]
    rows: List[Dict[str, Any]]

class MappingPayloadSchema(BaseModel):
    rule_field: str
    mapped_column: Any # Can be str or None
    confidence: float
    match_type: str

class ComplianceRequest(BaseModel):
    dataset: DatasetPayloadSchema
    rules: List[str]
    mappings: List[MappingPayloadSchema]

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/check")
async def check_compliance(request: ComplianceRequest):
    """
    Evaluates a standardized dataset against an array of policy rules 
    and returns a summarized report of violations.
    """
    try:
        # 1. Transform Pydantic mapping schemas into raw dictionaries for the orchestrator
        # This keeps the business logic decoupled from the framing framework
        mappings_raw = [m.model_dump() for m in request.mappings]
        dataset_raw = request.dataset.model_dump()
        
        # 2. Execute orchestration
        compliance_report = run_compliance_check(
            dataset=dataset_raw,
            rules_list=request.rules,
            mappings=mappings_raw
        )
        
        # 3. Return results as JSON
        return compliance_report
        
    except ValueError as e:
        # Invalid rule format or evaluation failure (e.g., bad operators)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Generic fallback for systemic data failure
        raise HTTPException(status_code=500, detail=f"Compliance check engine failure: {str(e)}")
