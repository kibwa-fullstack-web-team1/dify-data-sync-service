from fastapi import FastAPI
from app.config.config import Config
from app.utils.logger import setup_logging
from app.utils.db import Base, engine, SessionLocal, get_db # get_db 임포트
from app.core.story_sync_service import sync_stories_for_senior # sync_stories 대신 sync_stories_for_senior 임포트
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
        # db 세션을 직접 전달하는 대신, 함수 내부에서 세션을 얻도록 변경
        # 테스트를 위해 1번 시니어 사용자의 ID를 사용합니다.
        scheduler.add_job(sync_stories_for_senior, 'interval', minutes=1, args=[1, None]) # senior_user_id를 먼저 전달, 1분 주기로 변경
        scheduler.start()
        logger.info("Scheduler started for story synchronization.")

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