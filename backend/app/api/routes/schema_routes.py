"""
schema_routes.py
----------------
REST API endpoints for automated schema mapping and rule field resolution.
"""

from typing import List
from fastapi import APIRouter, HTTPException, Query
from app.schema_mapper.contracts import SchemaMappingRequest, MappedColumnResponse
from app.schema_mapper.mapping_engine import map_fields

router = APIRouter()

@router.post("/map", response_model=List[MappedColumnResponse])
async def map_dataset_schema(
    request: SchemaMappingRequest,
    debug: bool = Query(False, description="If true, includes similarity rankings and threshold metrics in the response")
):
    """
    Analyzes an uploaded dataset's structure against a set of abstract rule requirements.
    
    Orchestrates a hybrid matching strategy:
    1. Exact Name Matching (Tier 1)
    2. Normalized Alphanumeric Matching (Tier 2)
    3. Semantic Vector Similarity (Tier 3)
    
    Returns a list of mapped column resolutions with confidence intervals.
    """
    try:
        # Extract the raw rule field strings from the contract request
        rule_fields = [r.field for r in request.rules]
        dataset_columns = request.dataset.columns
        
        # Execute the optimized batch mapping engine
        # includes debug metadata only if requested via query param
        mappings = map_fields(
            dataset_columns=dataset_columns,
            rule_fields=rule_fields,
            include_debug=debug
        )
        
        # map_fields result structure already aligns with MappedColumnResponse expectations
        return mappings
        
    except ValueError as e:
        # Catch internal processing errors (unsupported formats, empty lists, etc)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Fallback for severe model failures or session interruptions
        raise HTTPException(status_code=500, detail=f"Schema mapping pipeline failed: {str(e)}")
