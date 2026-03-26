"""
contracts.py
------------
Defines strict Pydantic data schemas representing the network payload contracts 
for mapping abstract compliance rules against concrete dataset architectures.

Includes pure structural mappings exclusively (no core business logic).
"""

from typing import List, Optional
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Input Contracts
# ---------------------------------------------------------------------------

class DatasetSchema(BaseModel):
    """
    Represents the structural metadata of a parsed dataset necessary for column mapping.
    """
    dataset_id: str = Field(..., description="Unique identifier associated with the uploaded dataset")
    columns: List[str] = Field(..., description="Complete list of literal column names scraped from the dataset")

class RuleFieldSchema(BaseModel):
    """
    Represents an independent rule abstraction requiring resolution.
    """
    field: str = Field(..., description="The abstract generic field name established by a policy rule")

class SchemaMappingRequest(BaseModel):
    """
    The parent request object sent to the mapping layer linking a target dataset architecture
    against an array of abstract rule requirements.
    """
    dataset: DatasetSchema
    rules: List[RuleFieldSchema]

# ---------------------------------------------------------------------------
# Output Contracts
# ---------------------------------------------------------------------------

class TopCandidate(BaseModel):
    column: str
    score: float

class DebugInfo(BaseModel):
    strategy_used: str = Field(..., description="The final matching layer reached: exact | normalized | semantic | none")
    threshold: float = Field(..., description="The semantic confidence floor used for this request")
    top_candidates: List[TopCandidate] = Field(..., description="Ranked list of prospective column matches")
    rejected_reason: Optional[str] = Field(None, description="Detailed explanation if no match was established (e.g., below_threshold)")

class MappedColumnResponse(BaseModel):
    """
    The finalized mapping linking a single abstract policy rule field intelligently 
    to a concrete dataset column with a confidence threshold.
    """
    rule_field: str = Field(..., description="The original abstract compliance field required")
    mapped_column: Optional[str] = Field(None, description="The strictly matched column name bound to the dataset, or None if no match")
    confidence: float = Field(..., description="Probability confidence interval (0.0 through 1.0) confirming exact match validity", ge=0.0, le=1.0)
    match_type: str = Field(..., description="Type of match: exact | normalized | semantic | none")
    debug: Optional[DebugInfo] = Field(None, description="Detailed debugging metrics injected only if requested")

# Note: Operations returning the final output should use `List[MappedColumnResponse]` 
# to enforce the required JSON Array wrapper constraint natively over HTTP APIs.
