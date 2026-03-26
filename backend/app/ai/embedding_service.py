"""
embedding_service.py
--------------------
AI Service responsible for generating vector embeddings for text elements.
Utilizes the sentence-transformers library with the all-MiniLM-L6-v2 model.
"""

from typing import List, Optional
from sentence_transformers import SentenceTransformer
import logging

# Configuration
MODEL_NAME = "all-MiniLM-L6-v2"

# Global model instance for singleton-like behavior
_model_instance: Optional[SentenceTransformer] = None

logger = logging.getLogger(__name__)

def _get_model() -> SentenceTransformer:
    """
    Initializes and returns the singleton SentenceTransformer model instance.
    The model is loaded only once on first access.
    """
    global _model_instance
    if _model_instance is None:
        logger.info(f"Loading transformer model: {MODEL_NAME}")
        try:
            _model_instance = SentenceTransformer(MODEL_NAME)
        except Exception as e:
            logger.error(f"Failed to load transformer model: {str(e)}")
            raise RuntimeError(f"Model initialization failure: {str(e)}")
    return _model_instance

def get_embedding(text: str) -> List[float]:
    """
    Generates a single vector embedding for the given input text.
    
    Args:
        text: The string to embed.
        
    Returns:
        List[float]: A vector of floats representing the text.
        Returns an empty list if text is empty or None.
    """
    if not text or not text.strip():
        return []
    
    model = _get_model()
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding.tolist()

def get_batch_embeddings(texts: List[str]) -> List[List[float]]:
    """
    Generates embeddings for a list of strings in batches.
    Optimizes for performance by passing the list directly to the model.
    
    Args:
        texts: A list of strings to embed.
        
    Returns:
        List[List[float]]: A list of float vectors.
        Returns an empty list for empty input.
    """
    if not texts:
        return []
        
    # Filter out empty/invalid strings but keep index alignment or handle them as empty vectors
    # Performance is best when passing the list directly
    model = _get_model()
    embeddings = model.encode(texts, convert_to_numpy=True)
    return embeddings.tolist()
