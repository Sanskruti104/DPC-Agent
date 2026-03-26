"""
similarity_service.py
---------------------
AI Service responsible for computing semantic similarity between rule fields 
and dataset columns using vector embeddings.
"""

from typing import List, Dict, Any
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from app.ai.embedding_service import get_embedding, get_batch_embeddings

def get_similarity_scores(
    rule_field: str, 
    dataset_columns: List[str], 
    column_embeddings: Optional[List[List[float]]] = None
) -> List[Dict[str, Any]]:
    """
    Computes semantic similarity scores between a rule field and all provided dataset columns.
    
    Args:
        rule_field: The abstract rule field name.
        dataset_columns: A list of actual column names from the dataset.
        column_embeddings: Optional precomputed embeddings for the dataset columns.
        
    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing:
            - column: The original column name.
            - score: The cosine similarity float (ranked highest to lowest).
    """
    if not rule_field or not dataset_columns:
        return []
        
    # 1. Generate embedding for rule_field
    rule_embedding = get_embedding(rule_field)
    if not rule_embedding:
        return []
        
    # 2. Use or generate embeddings for dataset_columns (batch)
    if column_embeddings is None:
        column_embeddings = get_batch_embeddings(dataset_columns)
        
    if not column_embeddings:
        return []
        
    # 3. Compute cosine similarity between rule_field and each column
    rule_vector = np.array(rule_embedding).reshape(1, -1)
    column_vectors = np.array(column_embeddings)
    
    # cosine_similarity returns an array of shape (1, len(column_vectors))
    similarities = cosine_similarity(rule_vector, column_vectors)[0]
    
    # 4. Pair each column with its similarity score
    results = []
    for col, score in zip(dataset_columns, similarities):
        results.append({
            "column": col,
            "score": float(score)  # Convert from numpy float for serialization
        })
        
    # 5. Return results sorted (highest score first)
    return sorted(results, key=lambda x: x["score"], reverse=True)
