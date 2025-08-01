import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_story_sequencer_db
from app.models.story_sequencer_models import Story
from typing import Optional

logger = logging.getLogger(__name__)


async def _get_last_synced_timestamp(db: Session, service_name: str) -> datetime:
    """마지막 동기화 타임스탬프를 가져옵니다."""
    metadata = db.query(SyncMetadata).filter_by(service_name=service_name).first()
    if metadata and metadata.last_synced_timestamp:
        return metadata.last_synced_timestamp
    # 초기 동기화를 위해 매우 오래된 타임스탬프 반환 (UTC로 설정)
    return datetime.min.replace(tzinfo=timezone.utc)

def _update_last_synced_timestamp(db: Session, service_name: str, new_timestamp: datetime):
    """마지막 동기화 타임스탬프를 업데이트합니다."""
    metadata = db.query(SyncMetadata).filter_by(service_name=service_name).first()
    if metadata:
        metadata.last_synced_timestamp = new_timestamp
    else:
        db.add(SyncMetadata(service_name=service_name, last_synced_timestamp=new_timestamp))
    db.commit()

async def _fetch_new_stories(
    db: Session,
    service_name: str,
    user_id: Optional[int] = None # user_id 파라미터 추가
) -> list:
    """story-sequencer DB에서 새로운 스토리를 가져옵니다."""
    last_synced = await _get_last_synced_timestamp(db, service_name)
    
    # story-sequencer DB에 직접 접근
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        query = story_sequencer_db.query(Story)
        query = query.filter(Story.updated_at >= last_synced.replace(tzinfo=None) if last_synced.tzinfo else last_synced)
        if user_id:
            query = query.filter(Story.user_id == user_id)
        stories = query.all()
        return [story.__dict__ for story in stories] # 딕셔너리 형태로 반환
    except Exception as e:
        logger.error(f"Error fetching stories directly from DB: {e}")
        return []
    finally:
        story_sequencer_db.close()

async def _fetch_all_story_ids(
    db: Session,
    user_id: Optional[int] = None # user_id 파라미터 추가
) -> list:
    """story-sequencer DB에서 모든 이야기 ID를 가져옵니다."""
    # story-sequencer DB에 직접 접근
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        query = story_sequencer_db.query(Story.id)
        if user_id:
            query = query.filter(Story.user_id == user_id)
        story_ids = query.all()
        return [story_id[0] for story_id in story_ids]
    except Exception as e:
        logger.error(f"Error fetching all story IDs directly from DB: {e}")
        return []
    finally:
        story_sequencer_db.close()

def _transform_story_to_dify_format(story: dict, service_name: str) -> dict:
    """스토리 데이터를 Dify Document Upload API 형식으로 변환합니다."""
    return {
        "text": story.get("content", ""),
        "meta": {
            "user_id": story.get("user_id"),
            "story_id": story.get("id"),
            "title": story.get("title", ""),
            "created_at": story.get("created_at"),
            "updated_at": story.get("updated_at"),
            "source": service_name
        }
    }

async def sync_stories(db: Session, user_id: Optional[int] = None):
    """스토리를 동기화하고 Dify 형식으로 변환합니다."""
    logger.info(f"Starting story synchronization for user_id: {user_id}...")
    
    service_name = "story-sequencer"

    # 1. 새로 생성되거나 업데이트된 스토리 동기화
    new_stories = await _fetch_new_stories(
        db, service_name, user_id # user_id 전달
    )
    
    if not new_stories:
        logger.info("No new stories to sync.")
    else:
        latest_timestamp = await _get_last_synced_timestamp(db, service_name)
        for story in new_stories:
            # Dify 업로드 로직 (다음 태스크에서 구현 예정)
            dify_data = _transform_story_to_dify_format(story, service_name)
            logger.info(f"Transformed story for Dify: {dify_data}")
            
            # 마지막 동기화 타임스탬프 업데이트
            story_updated_at = story["updated_at"] if "updated_at" in story else datetime.min
            
            # story_updated_at이 offset-naive일 경우 UTC로 변환
            if story_updated_at.tzinfo is None:
                story_updated_at = story_updated_at.replace(tzinfo=timezone.utc)

            if story_updated_at > latest_timestamp:
                latest_timestamp = story_updated_at

        _update_last_synced_timestamp(db, service_name, latest_timestamp)
        logger.info(f"Story synchronization completed. Last synced timestamp updated to {latest_timestamp}")

    # 2. 삭제된 스토리 처리 (스냅샷 비교)
    current_story_ids_on_sequencer = set(await _fetch_all_story_ids(db, user_id)) # user_id 전달
    
    metadata = db.query(SyncMetadata).filter_by(service_name=service_name).first()
    synced_story_ids_str = metadata.synced_story_ids if metadata and metadata.synced_story_ids else "[]"
    synced_story_ids_on_dify = set(json.loads(synced_story_ids_str))

    deleted_story_ids = synced_story_ids_on_dify - current_story_ids_on_sequencer

    if deleted_story_ids:
        logger.info(f"Found deleted stories: {deleted_story_ids}. Deleting from Dify...")
        for deleted_id in deleted_story_ids:
            # Dify에서 문서 삭제 로직 (다음 태스크에서 구현 예정)
            logger.info(f"[Dify Delete Placeholder] Deleting story with ID: {deleted_id} from Dify.")
    else:
        logger.info("No stories to delete from Dify.")

    # 3. 동기화된 ID 목록 업데이트
    updated_synced_story_ids = list(current_story_ids_on_sequencer)
    if metadata:
        metadata.synced_story_ids = json.dumps(updated_synced_story_ids)
    else:
        db.add(SyncMetadata(service_name=service_name, synced_story_ids=json.dumps(updated_synced_story_ids)))
    db.commit()
    logger.info("Synced story IDs updated in metadata.")

    logger.info("Full story synchronization process completed.")
