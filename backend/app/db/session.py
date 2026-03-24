from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

engine = create_engine(
    settings.DATABASE_URL,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 5
    },
    pool_pre_ping=True,
    echo=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
