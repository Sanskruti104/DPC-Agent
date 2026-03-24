from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.routes import health, user_routes
from app.db.base import Base
from app.db.session import engine
from app.models import user  # Ensure models are imported

def get_application() -> FastAPI:
    # Create tables automatically
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: automatic table creation failed: {e}")

    app = FastAPI(
        title=settings.PROJECT_NAME,
        openapi_url=f"{settings.API_V1_STR}/openapi.json"
    )

    # Set all CORS origins
    if settings.BACKEND_CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Include routers
    app.include_router(health.router, tags=["health"])
    app.include_router(user_routes.router, prefix=settings.API_V1_STR, tags=["users"])
    
    return app

app = get_application()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
