"""
dataset_parser.py
-----------------
Service to securely and efficiently parse incoming datasets into pandas DataFrames.
"""

import pandas as pd
from fastapi import UploadFile

def parse_dataset(file: UploadFile, file_type: str) -> pd.DataFrame:
    """
    Parses an UploadFile into a pandas DataFrame based on the detected file_type.
    
    Args:
        file: The FastAPI UploadFile object containing the dataset.
        file_type: Canonical file type string ("csv", "excel", or "json").
        
    Returns:
        pd.DataFrame: The uncleaned DataFrame extracted from the file without modifications.
        
    Raises:
        ValueError: If file_type is unsupported or if parsing fails.
    """
    # Ensure file pointer is at the beginning
    file.file.seek(0)
    
    try:
        if file_type == "csv":
            # Direct file-object read avoids loading raw bytes into memory at once
            df = pd.read_csv(file.file)
        elif file_type == "excel":
            # Reads directly from the SpooledTemporaryFile underlying UploadFile
            df = pd.read_excel(file.file)
        elif file_type == "json":
            df = pd.read_json(file.file)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
            
        if df.empty:
            raise ValueError("Empty dataset: No data rows found.")
            
    except pd.errors.EmptyDataError:
        raise ValueError("Empty dataset: The file is completely empty.")
    except ValueError as e:
        # Re-raise already formulated ValueErrors directly
        raise e
    except Exception as e:
        # Catch wide scope pandas parsing errors mapped heavily to 'corrupt' flows
        raise ValueError(f"Corrupt file or parsing failure: {str(e)}")
        
    finally:
        # Rewind pointer gracefully in case the file needs to be read again down the line
        file.file.seek(0)
        
    return df
