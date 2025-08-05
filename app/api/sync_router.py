from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.utils.db import get_db
from app.core.story_sync_service import sync_stories, rebuild_dataset, clear_dataset, get_user_llm_metadata, get_user_llm_context
import logging
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sync",
    tags=["Synchronization"]
)

@router.post("/stories")
async def trigger_story_sync(guardian_user_id: Optional[int] = None, db: Session = Depends(get_db)):
    """수동으로 story-sequencer 데이터 동기화를 트리거합니다."""
    logger.info(f"Manual story synchronization triggered for guardian_user_id: {guardian_user_id}.")
    try:
        await sync_stories(db, guardian_user_id)
        return {"message": "Story synchronization initiated successfully.", "status": "success"}
    except Exception as e:
        logger.error(f"Error during manual story synchronization: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Synchronization failed: {e}")

@router.delete("/stories/dataset")
async def trigger_clear_dataset(guardian_user_id: int, db: Session = Depends(get_db)):
    """특정 사용자의 Dify 데이터셋에 있는 모든 문서를 삭제하고 동기화 상태를 초기화합니다."""
    logger.info(f"Dataset clearing triggered for guardian_user_id: {guardian_user_id}.")
    try:
        success = await clear_dataset(db, guardian_user_id)
        if success:
            return {"message": f"Dataset for guardian_user_id {guardian_user_id} cleared successfully.", "status": "success"}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset or user not found.")
    except Exception as e:
        logger.error(f"Error during dataset clearing: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Dataset clearing failed: {e}")

@router.post("/stories/rebuild")
async def trigger_rebuild_dataset(guardian_user_id: int, db: Session = Depends(get_db)):
    """특정 사용자의 Dify 데이터셋을 비우고 모든 데이터를 처음부터 다시 동기화합니다."""
    logger.info(f"Dataset rebuild triggered for guardian_user_id: {guardian_user_id}.")
    try:
        await rebuild_dataset(db, guardian_user_id)
        return {"message": f"Dataset for guardian_user_id {guardian_user_id} is being rebuilt.", "status": "success"}
    except Exception as e:
        logger.error(f"Error during dataset rebuild: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Dataset rebuild failed: {e}")

@router.get("/users/{guardian_user_id}/llm-metadata")
async def get_llm_metadata_for_user(guardian_user_id: int, db: Session = Depends(get_db)):
    """특정 사용자의 LLM 프롬프트 활용 메타데이터를 조회합니다."""
    metadata = await get_user_llm_metadata(db, guardian_user_id)
    if not metadata:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LLM metadata not found for this user.")
    return metadata

@router.post("/users/{guardian_user_id}/llm-context")
async def get_llm_context_for_user(guardian_user_id: int, query: str, top_k: int = 3, db: Session = Depends(get_db)):
    """특정 사용자의 Dify 데이터셋에서 LLM 컨텍스트를 검색합니다."""
    context = await get_user_llm_context(db, guardian_user_id, query, top_k)
    if not context:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No LLM context found for this user and query.")
    return {"context": context}