"""
dataset_routes.py
-----------------
Module 5 — Dataset Upload & Parsing endpoint.

Provides REST API endpoints for uploading transaction/data files
(CSV or Excel) that will later be validated against extracted policy rules.

Supported formats:
  - .csv   → saved directly as CSV
  - .xlsx  → converted to CSV via pandas, then saved
  - .xls   → converted to CSV via pandas, then saved

Storage layout:
    uploads/
    └── datasets/
        └── <uuid>.csv

Pipeline position:
    policy_pipeline → rule_normalizer → query_builder → [this] → dataset_validator
"""

from __future__ import annotations

import io
import logging
import os
import shutil
import uuid
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Where all uploaded datasets are stored after conversion.
DATASET_DIR: Path = Path(settings.UPLOAD_DIR) / "datasets"

#: Allowed MIME types and their canonical format name.
_SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".csv":  "csv",
    ".xlsx": "excel",
    ".xls":  "excel",
}

#: Maximum allowed file size (50 MB). Prevents runaway uploads.
MAX_FILE_SIZE_BYTES: int = 50 * 1024 * 1024


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------

class DatasetUploadResponse(BaseModel):
    """Response returned after a successful dataset upload."""

    dataset_id:        str
    """UUID assigned to this dataset — used in all subsequent API calls."""

    original_filename: str
    """The filename that was sent by the client."""

    stored_filename:   str
    """The final filename written to disk (always ``<uuid>.csv``)."""

    file_type:         str
    """Canonical file type: ``'csv'`` or ``'excel'``."""

    num_rows:          int
    """Number of data rows parsed from the file (header excluded)."""

    num_columns:       int
    """Number of columns present in the dataset."""

    columns:           list[str]
    """List of column names exactly as they appear in the file."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_dataset_dir() -> None:
    """Create the dataset storage directory if it does not yet exist."""
    DATASET_DIR.mkdir(parents=True, exist_ok=True)


def _get_extension(filename: str) -> str:
    """Return the lowercase file extension including the leading dot."""
    return Path(filename).suffix.lower()


def _validate_file(file: UploadFile, raw_bytes: bytes) -> str:
    """
    Validate the uploaded file and return its canonical type.

    Args:
        file:      The FastAPI ``UploadFile`` object.
        raw_bytes: The complete file content already read into memory.

    Returns:
        Canonical type string: ``"csv"`` or ``"excel"``.

    Raises:
        HTTPException 400: On unsupported extension or empty file.
    """
    filename = file.filename or ""

    # 1. Reject missing filename
    if not filename:
        raise HTTPException(status_code=400, detail="Uploaded file has no filename.")

    # 2. Check extension
    ext = _get_extension(filename)
    if ext not in _SUPPORTED_EXTENSIONS:
        allowed = ", ".join(_SUPPORTED_EXTENSIONS)
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file type '{ext}'. "
                f"Allowed extensions: {allowed}"
            ),
        )

    # 3. Reject empty files
    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # 4. Enforce max size
    if len(raw_bytes) > MAX_FILE_SIZE_BYTES:
        max_mb = MAX_FILE_SIZE_BYTES // (1024 * 1024)
        raise HTTPException(
            status_code=400,
            detail=f"File exceeds the maximum allowed size of {max_mb} MB.",
        )

    return _SUPPORTED_EXTENSIONS[ext]


def _parse_to_dataframe(raw_bytes: bytes, file_type: str, filename: str) -> pd.DataFrame:
    """
    Parse raw file bytes into a ``pandas.DataFrame``.

    Args:
        raw_bytes:  Complete file content.
        file_type:  ``"csv"`` or ``"excel"``.
        filename:   Original filename (used for error messages only).

    Returns:
        A ``DataFrame`` with at least one row and one column.

    Raises:
        HTTPException 400: If the file cannot be parsed or is empty after parsing.
        HTTPException 500: On unexpected I/O errors.
    """
    try:
        buffer = io.BytesIO(raw_bytes)

        if file_type == "csv":
            # Try UTF-8 first; fall back to latin-1 for legacy exports
            try:
                df = pd.read_csv(buffer, encoding="utf-8")
            except UnicodeDecodeError:
                buffer.seek(0)
                df = pd.read_csv(buffer, encoding="latin-1")

        else:  # excel
            df = pd.read_excel(buffer, engine="openpyxl" if filename.endswith(".xlsx") else "xlrd")

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to parse file '%s': %s", filename, exc)
        raise HTTPException(
            status_code=400,
            detail=f"Could not parse '{filename}': {exc}",
        ) from exc

    # Reject completely empty datasets
    if df.empty or df.shape[1] == 0:
        raise HTTPException(
            status_code=400,
            detail="The uploaded file contains no data (empty rows or columns).",
        )

    # Strip leading/trailing whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]

    return df


def _save_as_csv(df: pd.DataFrame, dataset_id: str) -> Path:
    """
    Persist a ``DataFrame`` as a UTF-8 CSV file inside :data:`DATASET_DIR`.

    Args:
        df:         The parsed dataset.
        dataset_id: UUID string used as the filename stem.

    Returns:
        The absolute :class:`~pathlib.Path` of the written CSV.

    Raises:
        HTTPException 500: On any I/O failure.
    """
    _ensure_dataset_dir()
    dest = DATASET_DIR / f"{dataset_id}.csv"

    try:
        df.to_csv(dest, index=False, encoding="utf-8")
    except Exception as exc:
        logger.exception("Failed to write dataset to disk: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Could not save dataset to disk: {exc}",
        ) from exc

    logger.info("Dataset saved: %s  (%d rows x %d cols)", dest.name, len(df), df.shape[1])
    return dest


# ---------------------------------------------------------------------------
# Synchronous processing function (runs in threadpool)
# ---------------------------------------------------------------------------

def _process_upload(raw_bytes: bytes, file_type: str, filename: str) -> tuple[pd.DataFrame, str]:
    """
    Parse the raw bytes into a DataFrame and write it as CSV.

    This function is CPU/IO-bound and is executed in a threadpool to keep
    the async event loop unblocked.

    Returns:
        ``(df, dataset_id)`` tuple.
    """
    df         = _parse_to_dataframe(raw_bytes, file_type, filename)
    dataset_id = str(uuid.uuid4())
    _save_as_csv(df, dataset_id)
    return df, dataset_id


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=DatasetUploadResponse, status_code=200)
async def upload_dataset(file: UploadFile = File(...)) -> DatasetUploadResponse:
    """
    Upload a transaction/data file for compliance validation.

    Accepts:
        - ``text/csv``  (``.csv``)
        - Excel 2007+   (``.xlsx``)
        - Excel 97-2003 (``.xls``)

    The file is parsed, normalized to CSV, and stored in
    ``uploads/datasets/<uuid>.csv``.  The returned ``dataset_id`` is used
    in subsequent calls to the validation engine.

    Returns:
        :class:`DatasetUploadResponse` with dataset metadata.

    Raises:
        **HTTP 400** — unsupported extension, empty file, parse failure.
        **HTTP 500** — unexpected server-side I/O error.
    """
    if not file:
        raise HTTPException(status_code=400, detail="No file was uploaded.")

    filename = file.filename or "unknown"
    logger.info("Dataset upload started: '%s'", filename)

    # 1. Read entire file into memory (prevents partial-read bugs with async)
    try:
        raw_bytes = await file.read()
    except Exception as exc:
        logger.exception("Failed to read uploaded file: %s", exc)
        raise HTTPException(status_code=500, detail=f"Could not read uploaded file: {exc}")

    # 2. Validate extension + size
    file_type = _validate_file(file, raw_bytes)

    # 3. Parse + Save (blocking I/O → threadpool)
    try:
        df, dataset_id = await run_in_threadpool(
            _process_upload, raw_bytes, file_type, filename
        )
    except HTTPException:
        raise  # re-raise our own controlled errors unchanged
    except Exception as exc:
        logger.exception("Unexpected error processing dataset: %s", exc)
        raise HTTPException(status_code=500, detail=f"Unexpected server error: {exc}")

    logger.info(
        "Dataset upload complete: id=%s  file='%s'  shape=(%d, %d)",
        dataset_id, filename, len(df), df.shape[1],
    )

    return DatasetUploadResponse(
        dataset_id        = dataset_id,
        original_filename = filename,
        stored_filename   = f"{dataset_id}.csv",
        file_type         = file_type,
        num_rows          = len(df),
        num_columns       = df.shape[1],
        columns           = df.columns.tolist(),
    )
