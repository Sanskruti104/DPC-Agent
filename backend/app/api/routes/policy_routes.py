import logging
from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models.policy import Policy
from app.schemas.policy import (
    PolicyUploadResponse, 
    PolicyProcessResponse,
    PolicyListResponse,
    PolicyDetail
)
from app.services.policy_service import save_policy_file
from app.services.policy_pipeline import process_policy

# Configure logger for route tracking
logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/", response_model=PolicyListResponse)
def list_policies(db: Session = Depends(get_db)):
    """
    List all uploaded policies and their current status.
    """
    policies = db.query(Policy).order_by(Policy.created_at.desc()).all()
    return {"policies": policies}

@router.get("/{file_id}", response_model=PolicyDetail)
def get_policy(file_id: str, db: Session = Depends(get_db)):
    """
    Get detailed information about a specific policy.
    """
    policy = db.query(Policy).filter(Policy.file_id == file_id).first()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    return policy

@router.post("/upload", response_model=PolicyUploadResponse)
async def upload_policy(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Ingest a policy PDF file for analysis and save metadata to the database.
    """
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    # 1. Run blocking storage I/O first
    logger.info(f"💾 Ingesting file: {file.filename}")
    file_id = await run_in_threadpool(save_policy_file, file)
    
    # 2. Record this in the database
    new_policy = Policy(
        file_id=file_id,
        filename=file.filename,
        status="pending"
    )
    db.add(new_policy)
    db.commit()
    db.refresh(new_policy)
    
    return PolicyUploadResponse(
        file_id=file_id,
        filename=file.filename
    )

@router.post("/process/{file_id}", response_model=PolicyProcessResponse)
async def process_policy_endpoint(file_id: str, db: Session = Depends(get_db)):
    """
    Execute the full policy processing pipeline for a given file_id:
    PDF Extraction -> Text Normalization -> NLP Clause Segmentation.
    """
    # 1. Verify existence and update status
    policy = db.query(Policy).filter(Policy.file_id == file_id).first()
    if not policy:
        raise HTTPException(status_code=404, detail="File ID not found in database registry")
        
    policy.status = "processing"
    db.commit()
    
    # 2. Orchestrate heavy work
    logger.info(f"⚙️ Processing request for file_id={file_id}")
    try:
        outcome = await run_in_threadpool(process_policy, file_id)
        
        # 3. Persist extraction results
        policy.status = "completed"
        policy.num_clauses = outcome.get("num_clauses", 0)
        policy.clauses = outcome.get("clauses", [])
        db.commit()
        
        return PolicyProcessResponse(**outcome)
        
    except Exception as e:
        policy.status = "failed"
        db.commit()
        logger.error(f"❌ Processing failed for {file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

import io
import pdfplumber

@router.post("/extract")
async def extract_clauses_direct(file: UploadFile = File(...)):
    """
    Accepts a PDF file upload and extracts clauses by reading all pages via pdfplumber
    and splitting the resulting text by newline. Returns a clean list of clauses.
    """
    if not file or not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Please upload a valid PDF document.")
    
    try:
        # 1. Read the uploaded file into an in-memory byte stream
        file_content = await file.read()
        
        extracted_text = ""
        
        # 2. Extract text from all pages using pdfplumber
        with pdfplumber.open(io.BytesIO(file_content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    extracted_text += page_text + "\n"
        
        # 3. Process the text into clean clauses (split by newline, remove empty lines)
        clauses = [
            line.strip() 
            for line in extracted_text.split("\n") 
            if line.strip()
        ]
        
        return {"clauses": clauses}
        
    except Exception as e:
        logger.error(f"❌ PDF Extraction failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to parse PDF: {str(e)}")
