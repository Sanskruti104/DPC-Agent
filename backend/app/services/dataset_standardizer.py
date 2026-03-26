"""
dataset_standardizer.py
-----------------------
Service to globally standardize parsed DataFrames into a reliable dictionary structure securely built for compliance engines.
"""

import uuid
import pandas as pd
from typing import Any, Dict

def standardize_dataset(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Converts a pandas DataFrame into a standardized dictionary output format, 
    dynamically inferring schema without destroying raw values.
    
    Args:
        df: The input pandas DataFrame.
        
    Returns:
        dict: Containing the properties `dataset_id`, `columns`, and `rows`.
    """
    # 1. Provide an auto-generated id
    dataset_id = str(uuid.uuid4())
    
    # 2. Extract column headers cleanly
    columns = df.columns.tolist()
    
    # 3. Sanitize nulls securely to None without dropping rows or converting types maliciously.
    # By mapping NaNs into None, we preserve standard Python NoneTypes ensuring safety for upstream routing/json serialization.
    # It ensures nulls are *not* dropped, as required.
    df_safe = df.where(pd.notnull(df), None)
    
    # 4. Orient the structure into rows of dictionaries {column: value}
    rows = df_safe.to_dict(orient="records")
    
    return {
        "dataset_id": dataset_id,
        "columns": columns,
        "rows": rows
    }
