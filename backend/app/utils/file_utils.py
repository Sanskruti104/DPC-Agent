"""
file_utils.py
-------------
Utility functions for file handling and validation.
"""

import pathlib
from typing import Union
from fastapi import UploadFile

SUPPORTED_EXTENSIONS = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json"
}

def detect_file_type(file_input: Union[str, UploadFile]) -> str:
    """
    Detects the file type based on the extension of the provided filename or UploadFile.
    
    Args:
        file_input: A filename string or a FastAPI UploadFile object.
        
    Returns:
        A canonical file type string ("csv", "excel", or "json").
        
    Raises:
        ValueError: If the file type is entirely unsupported or filename is missing.
    """
    if isinstance(file_input, UploadFile):
        filename = file_input.filename
    else:
        filename = file_input
        
    if not filename:
        raise ValueError("Provided file input has no filename.")
        
    ext = pathlib.Path(filename).suffix.lower()
    
    if ext not in SUPPORTED_EXTENSIONS:
        allowed = ", ".join(SUPPORTED_EXTENSIONS.keys())
        raise ValueError(f"Unsupported file type '{ext}'. Allowed: {allowed}")
        
    return SUPPORTED_EXTENSIONS[ext]
