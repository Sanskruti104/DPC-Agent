import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Ensure we load from the backend directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

class Settings(BaseSettings):
    PROJECT_NAME: str = "Policy Compliance Agent"
    API_V1_STR: str = "/api/v1"
    
    # CORS Configuration
    BACKEND_CORS_ORIGINS: list[str] = ["*"]
    # Database Configuration
    DATABASE_URL: str | None = os.getenv("DATABASE_URL")
    
    class Config:
        env_file = env_path
        case_sensitive = True
        extra = "ignore"

settings = Settings()

print("Loaded DATABASE_URL:", settings.DATABASE_URL)
