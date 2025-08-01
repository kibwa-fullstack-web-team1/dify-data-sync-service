from fastapi import FastAPI
from app.config.config import Config
from app.utils.logger import setup_logging
from app.utils.db import Base, engine, SessionLocal
from app.core.story_sync_service import StorySyncService
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

logger = logging.getLogger(__name__)

def create_app():
    setup_logging()
    app = FastAPI()
    scheduler = AsyncIOScheduler()

    @app.on_event("startup")
    async def startup_event():
        logger.info("Application startup event triggered.")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/checked.")

        # StorySyncService 초기화 및 스케줄러 등록
        db = SessionLocal()
        try:
            story_sync_service = StorySyncService(db)
            # 매 5분마다 story_sync_service.sync_stories 호출
            scheduler.add_job(story_sync_service.sync_stories, 'interval', minutes=5)
            scheduler.start()
            logger.info("Scheduler started for story synchronization.")
        finally:
            db.close()

    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("Application shutdown event triggered.")
        if scheduler.running:
            scheduler.shutdown()
            logger.info("Scheduler shut down.")

    @app.get("/")
    async def root():
        return {"message": "Dify Data Sync Service is running"}

    return app