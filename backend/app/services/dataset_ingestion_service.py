"""
dataset_ingestion_service.py
----------------------------
Business logic orchestrator governing the secure ingestion, standardization, and profiling workflows for raw dataset payloads.
"""

from typing import Any, Dict
from fastapi import UploadFile

# Pipeline component imports
from app.utils.file_utils import detect_file_type
from app.services.dataset_parser import parse_dataset
from app.services.dataset_standardizer import standardize_dataset
from app.services.dataset_metadata import extract_metadata

def ingest_dataset(file: UploadFile) -> Dict[str, Any]:
    """
    Orchestrates the workflow for processing raw UploadFiles sequentially:
      UploadFile → detect type → parse → standardize → extract metadata.
      
    Args:
        file: The incoming FastAPI UploadFile object.
        
    Returns:
        dict: A finalized standard structure dict incorporating dataset components and profiling.
              {
                "dataset_id": str,
                "columns": list,
                "rows": list(dict),
                "metadata": dict
              }
              
    Raises:
        ValueError: Propagates validation or parsing exceptions up to the routing layer cleanly.
    """
    
    # Step 1: Detect file protocol based on filename
    file_type = detect_file_type(file)
    
    # Step 2: Parse standard UploadFile bytes into unmodified pandas DataFrame
    df = parse_dataset(file, file_type)
    
    # Step 3: Run DataFrame array normalizations into rows and fetch standard components
    standardized_structure = standardize_dataset(df)
    
    # Step 4: Harvest dimensional layouts cleanly
    metadata = extract_metadata(df)
    
    # Step 5: Consolidate Final Architecture Response
    return {
        "dataset_id": standardized_structure["dataset_id"],
        "columns": standardized_structure["columns"],
        "rows": standardized_structure["rows"],
        "metadata": metadata
    }
