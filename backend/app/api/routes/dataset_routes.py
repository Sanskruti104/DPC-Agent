"""
dataset_routes.py
-----------------
REST API endpoints for completely uploading and ingesting structured datasets.
"""

from typing import Any, Dict, List
from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from app.services.dataset_ingestion_service import ingest_dataset

router = APIRouter()

# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class MetadataResponse(BaseModel):
    total_rows: int
    total_columns: int
    column_types: Dict[str, str]

class DatasetIngestionResponse(BaseModel):
    dataset_id: str
    columns: List[str]
    rows: List[Dict[str, Any]]  # Maps {column: raw_value} where raw_value can be safely serialized to JSON
    metadata: MetadataResponse

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=DatasetIngestionResponse)
async def upload_dataset(file: UploadFile = File(...)):
    """
    Accepts an uploaded dataset, orchestrates type detection, memory-efficient
    DataFrame parsing, and JSON standardizations without relying on heavy data structures.
    """
    if not file:
        raise HTTPException(status_code=400, detail={"error": "File missing", "details": "No dataset was provided."})
        
    try:
        # Orchestrate the entire ingestion without bleeding business logic here
        ingestion_result = ingest_dataset(file)
    except ValueError as e:
        error_msg = str(e)
        
        # Parse internal errors securely into requested dictionary structures
        if "Unsupported file type" in error_msg or "Unsupported" in error_msg:
            raise HTTPException(status_code=415, detail={"error": "Unsupported file type", "details": error_msg})
        elif "Empty dataset" in error_msg:
            raise HTTPException(status_code=422, detail={"error": "Empty dataset", "details": error_msg})
        elif "Corrupt file" in error_msg:
            raise HTTPException(status_code=400, detail={"error": "Corrupt file", "details": error_msg})
        else:
            raise HTTPException(status_code=400, detail={"error": "Parsing failure", "details": error_msg})
            
    except Exception as e:
        # Broad spectrum fallback for severe data failures (pandas crash, out of memory, read interruptions)
        raise HTTPException(status_code=500, detail={"error": "Unexpected processing failure", "details": str(e)})
        
    return DatasetIngestionResponse(
        dataset_id=ingestion_result["dataset_id"],
        columns=ingestion_result["columns"],
        rows=ingestion_result["rows"],
        metadata=ingestion_result["metadata"]
    )
