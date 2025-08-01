from fastapi import FastAPI
from app.config.config import Config
from app.utils.logger import setup_logging
from app.utils.db import Base, engine
import logging

logger = logging.getLogger(__name__)

def create_app():
    setup_logging()
    app = FastAPI()

    @app.on_event("startup")
    async def startup_event():
        logger.info("Application startup event triggered.")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/checked.")

    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("Application shutdown event triggered.")

    @app.get("/")
    async def root():
        return {"message": "Dify Data Sync Service is running"}

    return app
