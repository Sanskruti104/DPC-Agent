from sqlalchemy.orm import Session
from app.models.user import User
from app.schemas.user import UserCreate

def create_user(db: Session, user_data: UserCreate):
    db_user = User(email=user_data.email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def get_all_users(db: Session):
    return db.query(User).all()
