from fastapi import FastAPI
from app.config.config import Config
from app.utils.logger import setup_logging
from app.utils.db import Base, engine, SessionLocal
from app.core.story_sync_service import sync_stories # StorySyncService 대신 sync_stories 함수 임포트
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.api.sync_router import router as sync_router # sync_router 임포트
import logging

logger = logging.getLogger(__name__)

def create_app():
    setup_logging()
    app = FastAPI()
    scheduler = AsyncIOScheduler()

    app.include_router(sync_router) # sync_router 포함

    @app.on_event("startup")
    async def startup_event():
        logger.info("Application startup event triggered.")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/checked.")

        # 스케줄러 등록
        db = SessionLocal()
        try:
            # 매 5분마다 sync_stories 함수 호출, db 세션과 user_id=None을 인자로 전달
            scheduler.add_job(sync_stories, 'interval', minutes=5, args=[db, None])
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