from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.api.deps import get_db

router = APIRouter()

@router.get("/health")
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return {"status": "ok", "message": "API is healthy"}

@router.get("/test-db")
def test_db_connection(db: Session = Depends(get_db)):
    """
    Endpoint to test the database connection by executing a simple SELECT 1 query.
    """
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "message": "Database connection successful"}
    except Exception as e:
        return {"status": "error", "message": f"Database connection failed: {str(e)}"}
