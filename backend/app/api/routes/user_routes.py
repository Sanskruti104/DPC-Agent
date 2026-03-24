from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.user import UserCreate, UserResponse
from app.services import user_service
from app.models.user import User

router = APIRouter()

@router.post("/users", response_model=UserResponse)
def create_user(user_in: UserCreate, db: Session = Depends(get_db)):
    """
    Create a new user.
    """
    # Optional: check if email exists to avoid a 500 error from unique constraint
    existing_user = db.query(User).filter(User.email == user_in.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
        
    return user_service.create_user(db=db, user_data=user_in)

@router.get("/users", response_model=List[UserResponse])
def get_all_users(db: Session = Depends(get_db)):
    """
    List all users.
    """
    return user_service.get_all_users(db=db)
