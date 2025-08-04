from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.utils.db import get_db
from app.core.story_sync_service import sync_stories, rebuild_dataset, clear_dataset # 신규 함수 임포트
import logging
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sync",
    tags=["Synchronization"]
)

@router.post("/stories")
async def trigger_story_sync(user_id: Optional[int] = None, db: Session = Depends(get_db)):
    """수동으로 story-sequencer 데이터 동기화를 트리거합니다."""
    logger.info(f"Manual story synchronization triggered for user_id: {user_id}.")
    try:
        await sync_stories(db, user_id)
        return {"message": "Story synchronization initiated successfully.", "status": "success"}
    except Exception as e:
        logger.error(f"Error during manual story synchronization: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Synchronization failed: {e}")

@router.delete("/stories/dataset")
async def trigger_clear_dataset(user_id: int, db: Session = Depends(get_db)):
    """특정 사용자의 Dify 데이터셋에 있는 모든 문서를 삭제하고 동기화 상태를 초기화합니다."""
    logger.info(f"Dataset clearing triggered for user_id: {user_id}.")
    try:
        success = await clear_dataset(db, user_id)
        if success:
            return {"message": f"Dataset for user_id {user_id} cleared successfully.", "status": "success"}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset or user not found.")
    except Exception as e:
        logger.error(f"Error during dataset clearing: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Dataset clearing failed: {e}")

@router.post("/stories/rebuild")
async def trigger_rebuild_dataset(user_id: int, db: Session = Depends(get_db)):
    """특정 사용자의 Dify 데이터셋을 비우고 모든 데이터를 처음부터 다시 동기화합니다."""
    logger.info(f"Dataset rebuild triggered for user_id: {user_id}.")
    try:
        await rebuild_dataset(db, user_id)
        return {"message": f"Dataset for user_id {user_id} is being rebuilt.", "status": "success"}
    except Exception as e:
        logger.error(f"Error during dataset rebuild: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Dataset rebuild failed: {e}")
