from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.utils.db import get_db
from app.core.story_sync_service import sync_stories # StorySyncService 대신 sync_stories 함수 임포트
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sync",
    tags=["Synchronization"]
)

@router.post("/stories")
async def trigger_story_sync(db: Session = Depends(get_db)):
    """수동으로 story-sequencer 데이터 동기화를 트리거합니다."""
    logger.info("Manual story synchronization triggered.")
    try:
        await sync_stories(db) # sync_stories 함수 직접 호출
        return {"message": "Story synchronization initiated successfully.", "status": "success"}
    except Exception as e:
        logger.error(f"Error during manual story synchronization: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Synchronization failed: {e}")