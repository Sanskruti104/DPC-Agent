"""
dataset_metadata.py
-------------------
Service to extract lightweight structural metadata from pandas DataFrames without heavy profiling.
"""

import pandas as pd
from typing import Any, Dict

def extract_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Extracts lightweight metadata from a pandas DataFrame.
    
    Args:
        df: The input pandas DataFrame.
        
    Returns:
        dict: A dictionary containing:
            - total_rows: The number of dataset rows.
            - total_columns: The number of dataset columns.
            - column_types: A dictionary mapping column names to their string dtype representations.
    """
    # 1. Total rows
    total_rows = len(df)
    
    # 2. Total columns
    total_columns = len(df.columns)
    
    # 3. Extract column datatypes cleanly
    # Use `.dtypes.items()` to extract O(1) metadata without scanning individual rows.
    # Convert keys/values to standard python strings to ensure JSON serialization compatibility.
    column_types = {str(col): str(dtype) for col, dtype in df.dtypes.items()}
    
    return {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "column_types": column_types
    }
