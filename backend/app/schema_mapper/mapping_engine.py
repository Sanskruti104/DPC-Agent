"""
mapping_engine.py
-----------------
Core orchestration engine for schema mapping.
Combines deterministic matchers (exact, normalized) with 
probabilistic semantic similarity as a fall-back.
"""

from typing import List, Optional, Dict, Any
from app.schema_mapper.matchers.exact_matcher import exact_match
from app.schema_mapper.matchers.normalized_matcher import normalized_match
from app.ai.similarity_service import get_similarity_scores

from app.ai.embedding_service import get_batch_embeddings

def map_single_field(
    rule_field: str, 
    dataset_columns: List[str], 
    semantic_threshold: float = 0.5,
    column_embeddings: Optional[List[List[float]]] = None,
    include_debug: bool = False
) -> Dict[str, Any]:
    """
    Orchestrates the hybrid matching strategy for a single rule field.
    
    1. Exact Match: 1:1 casing/whitespace (Confidence 1.0)
    2. Normalized Match: Ignores special chars and casing (Confidence 0.9)
    3. Semantic Match: Vector similarity ranking (Confidence = Similarity Score)
    
    Args:
        rule_field: The abstract name from policy rules.
        dataset_columns: List of columns available in the uploaded dataset.
        semantic_threshold: Confidence floor for semantic matches (Default 0.5).
        column_embeddings: Optional precomputed embeddings for dataset_columns.
        include_debug: Whether to inject extra metadata (match_type, similarities).
        
    Returns:
        Dict: Final mapping result containing rule_field, mapped_column, confidence, and match_type.
    """
    # Initialize basic mapping result
    result = {
        "rule_field": rule_field,
        "mapped_column": None,
        "confidence": 0.0,
        "match_type": "none"
    }
    
    if not rule_field or not dataset_columns:
        return result
        
    # Phase 3 (Similarity) pre-fetch if debugging is needed or deterministic checks fail
    # In a real batch, column_embeddings is provided by map_fields() once.
    similarity_rankings = get_similarity_scores(
        rule_field, 
        dataset_columns, 
        column_embeddings=column_embeddings
    )
    
    top_match = similarity_rankings[0] if similarity_rankings else {"column": None, "score": 0.0}

    # Phase 1: Exact
    exact_match_col = exact_match(rule_field, dataset_columns)
    if exact_match_col:
        result.update({
            "mapped_column": exact_match_col,
            "confidence": 1.0,
            "match_type": "exact"
        })
    # Phase 2: Normalized
    else:
        normalized_match_col = normalized_match(rule_field, dataset_columns)
        if normalized_match_col:
            result.update({
                "mapped_column": normalized_match_col,
                "confidence": 0.9,
                "match_type": "normalized"
            })
        # Phase 3: Semantic
        elif top_match["score"] >= semantic_threshold:
            result.update({
                "mapped_column": top_match["column"],
                "confidence": top_match["score"],
                "match_type": "semantic"
            })

    # Inject Debug Metadata if requested (Agent-like Explainability)
    if include_debug:
        rejected_reason = None
        if result["match_type"] == "none":
            if not similarity_rankings:
                rejected_reason = "no_columns_available"
            elif top_match["score"] < semantic_threshold:
                rejected_reason = "below_threshold"
            else:
                rejected_reason = "general_processing_failure"

        result["debug"] = {
            "strategy_used": result["match_type"],
            "threshold": semantic_threshold,
            "top_candidates": [
                {"column": r["column"], "score": r["score"]} 
                for r in similarity_rankings[:5]
            ],
            "rejected_reason": rejected_reason
        }
        
    return result

def map_fields(
    dataset_columns: List[str], 
    rule_fields: List[str],
    semantic_threshold: float = 0.5,
    include_debug: bool = False
) -> List[Dict[str, Any]]:
    """
    Maps multiple rule fields to dataset columns efficiently with optional debug metadata.
    
    Args:
        dataset_columns: List of available dataset columns.
        rule_fields: List of abstract fields from policy rules.
        semantic_threshold: Threshold for semantic matching.
        include_debug: Boolean flag to include debugging metrics.
        
    Returns:
        List[Dict]: A list of mapping result dictionaries for each rule field.
    """
    if not dataset_columns or not rule_fields:
        return []
        
    # Optimization: Cache dataset column embeddings once
    cached_embeddings = get_batch_embeddings(dataset_columns)
    
    results = []
    for field in rule_fields:
        mapping = map_single_field(
            rule_field=field,
            dataset_columns=dataset_columns,
            semantic_threshold=semantic_threshold,
            column_embeddings=cached_embeddings,
            include_debug=include_debug
        )
        results.append(mapping)
        
    return results
