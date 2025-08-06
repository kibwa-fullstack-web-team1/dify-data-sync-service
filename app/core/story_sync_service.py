import logging
import re
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_story_sequencer_db, get_db as get_dify_db
from app.models.story_sequencer_models import Story
from typing import Optional, Dict, Any, List
import httpx
import asyncio

from app.core.user_service_client import get_senior_id_from_guardian_id, get_guardian_ids_for_senior
from app.core.dify_client import (
    upload_file_and_get_id,
    create_document_from_file,
    delete_document_from_dify,
    get_user_llm_metadata,
    get_user_llm_context
)

logger = logging.getLogger(__name__)

# --- Helper Functions ---

async def _get_last_synced_timestamp(db: Session, user_id: int, service_name: str) -> datetime:
    metadata = db.query(SyncMetadata).filter_by(user_id=user_id, service_name=service_name).first()
    if metadata and metadata.last_synced_timestamp:
        return metadata.last_synced_timestamp
    return datetime.min.replace(tzinfo=timezone.utc)

def _update_last_synced_timestamp(db: Session, metadata: SyncMetadata, new_timestamp: datetime):
    metadata.last_synced_timestamp = new_timestamp
    db.commit()

async def _fetch_stories(db: Session, user_ids: List[int], service_name: str, full_resync: bool = False) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        if full_resync:
            logger.info(f"Fetching all stories for user_ids: {user_ids} for full resync.")
            query = story_sequencer_db.query(Story).filter(Story.user_id.in_(user_ids))
        else:
            latest_synced_time = datetime.min.replace(tzinfo=timezone.utc)
            # senior_user_id를 기준으로 마지막 동기화 시간을 가져옵니다.
            # story_creator_ids에 포함된 모든 사용자는 동일한 senior_user_id를 공유하므로, 대표 ID 하나로 조회합니다.
            # 여기서는 user_ids[0]를 사용하지만, 실제로는 senior_user_id를 직접 사용하는 것이 더 명확합니다.
            # 이 함수는 sync_stories_for_senior에서 호출되므로, senior_user_id를 인자로 받는 것이 좋습니다.
            # 지금은 기존 구조를 유지하고, user_ids 리스트의 첫번째 사용자를 기준으로 조회합니다.
            if user_ids:
                # user_id가 아닌 senior_user_id를 기준으로 timestamp를 가져와야 합니다.
                # 이 함수를 호출하는 곳에서 senior_user_id를 알고 있으므로, 그 값을 사용해야 합니다.
                # 현재 구조에서는 user_ids에 시니어와 보호자가 섞여있으므로, 이 문제를 해결해야 합니다.
                # 가장 간단한 해결책은 이 함수의 호출부에서 senior_id를 넘겨주는 것입니다.
                # 지금은 첫번째 user_id로 가정하고 진행하지만, 리팩토링이 필요한 부분입니다.
                pass # timestamp 로직은 sync_stories_for_senior에서 직접 처리

            logger.info(f"Fetching new stories for user_ids: {user_ids}.")
            # last_synced_timestamp는 senior_user_id 기준으로 관리되므로, 개별 스토리를 시간으로 필터링하는 로직을 수정합니다.
            # full_resync가 아닐 경우, updated_at을 기준으로 새로운 스토리를 가져옵니다.
            # 이 로직은 sync_stories_for_senior에서 처리되므로 여기서는 모든 스토리를 가져옵니다.
            query = story_sequencer_db.query(Story).filter(Story.user_id.in_(user_ids))

        stories = query.all()
        return [story.__dict__ for story in stories]
    finally:
        story_sequencer_db.close()

async def _fetch_all_story_ids_for_user(user_ids: List[int]) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        query = story_sequencer_db.query(Story.id).filter(Story.user_id.in_(user_ids))
        return [story_id[0] for story_id in query.all()]
    finally:
        story_sequencer_db.close()

def _sanitize_title_for_filename(title: str) -> str:
    if not title:
        return ""
    title = re.sub(r'[\\/:*?"<>|]', '', title)
    title = re.sub(r'\s+', '_', title)
    return title[:50]

def _transform_story_to_dify_format(story: dict, senior_user_id: int, service_name: str) -> dict:
    created_at_iso = story.get("created_at").isoformat() if story.get("created_at") else None
    updated_at_iso = story.get("updated_at").isoformat() if story.get("updated_at") else None
    author_user_id = story.get("user_id")
    story_id = story.get("id")
    title = story.get("title", "")
    sanitized_title = _sanitize_title_for_filename(title)

    file_name = f"user_{author_user_id}_story_{story_id}_{sanitized_title}"

    return {
        "text": story.get("content", ""),
        "name": file_name,
        "meta": {
            "service_name": service_name,
            "author_user_id": author_user_id,
            "senior_user_id": senior_user_id,
            "story_id": story_id,
            "title": title,
            "created_at": created_at_iso,
            "updated_at": updated_at_iso,
            "source": "story-sequencer"
        },
        "indexing_technique": "high_quality"
    }

# --- Main Service Logic ---

async def sync_stories(db: Optional[Session] = None, guardian_user_id: Optional[int] = None, full_resync: bool = False):
    db_session = None
    try:
        if db is None:
            db_session = next(get_dify_db())
            db = db_session
        
        if guardian_user_id is None:
            logger.warning("sync_stories called without guardian_user_id. Skipping.")
            return

        senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
        if senior_user_id is None:
            logger.error(f"No senior user found for guardian_user_id: {guardian_user_id}. Aborting sync.")
            return

        await sync_stories_for_senior(senior_user_id, db=db, full_resync=full_resync)
        logger.info(f"Full story synchronization process completed for guardian_user_id: {guardian_user_id} (senior_user_id: {senior_user_id}).")
    finally:
        if db_session:
            db_session.close()

async def clear_dataset(db: Session, guardian_user_id: int) -> bool:
    db_session = None
    try:
        if db is None:
            db_session = next(get_dify_db())
            db = db_session
        
        logger.info(f"Clearing dataset for guardian_user_id: {guardian_user_id}")
        service_name = "story-sequencer"

        senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
        if senior_user_id is None:
            logger.error(f"No senior user found for guardian_user_id: {guardian_user_id}. Cannot clear dataset.")
            return False

        dataset_id = Config.DIFY_DATASET_ID
        if not dataset_id:
            logger.error("DIFY_DATASET_ID is not configured. Cannot clear dataset.")
            return False

        metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()
        if not metadata:
            logger.warning(f"No sync metadata found for senior_user_id {senior_user_id}. Nothing to clear.")
            return True

        synced_docs_list = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
        document_ids = [doc["dify_document_id"] for doc in synced_docs_list]

        async with httpx.AsyncClient() as client:
            delete_tasks = [delete_document_from_dify(client, dataset_id, doc_id) for doc_id in document_ids]
            await asyncio.gather(*delete_tasks)
        
        metadata.synced_story_ids = json.dumps([])
        metadata.last_synced_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        db.commit()
        logger.info(f"Successfully cleared all documents from dataset {dataset_id} for senior_user_id {senior_user_id}.")
        return True
    finally:
        if db_session:
            db_session.close()

async def rebuild_dataset(db: Session, guardian_user_id: int):
    db_session = None
    try:
        if db is None:
            db_session = next(get_dify_db())
            db = db_session
        
        logger.info(f"Rebuilding dataset for guardian_user_id: {guardian_user_id}")
        cleared = await clear_dataset(db, guardian_user_id)
        if cleared:
            await sync_stories(db, guardian_user_id, full_resync=True)
            logger.info(f"Dataset rebuild completed for guardian_user_id: {guardian_user_id}.")
        else:
            logger.error(f"Could not clear dataset for guardian_user_id {guardian_user_id}, rebuild aborted.")
    finally:
        if db_session:
            db_session.close()

async def sync_stories_for_senior(senior_user_id: int, db: Optional[Session] = None, full_resync: bool = False):
    db_session = None
    try:
        if db is None:
            db_session = next(get_dify_db())
            db = db_session
        
        logger.info(f"Starting story synchronization for senior_user_id: {senior_user_id} (full_resync: {full_resync})...")
        service_name = "story-sequencer"

        dataset_id = Config.DIFY_DATASET_ID
        if not dataset_id:
            logger.error("DIFY_DATASET_ID is not configured. Aborting sync.")
            return

        guardian_ids = await get_guardian_ids_for_senior(senior_user_id)
        story_creator_ids = [senior_user_id] + guardian_ids
        logger.info(f"Story creator IDs for senior_user_id {senior_user_id}: {story_creator_ids}")

        metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()
        if not metadata:
            metadata = SyncMetadata(user_id=senior_user_id, service_name=service_name, synced_story_ids=json.dumps([]))
            db.add(metadata)
            db.commit()
            db.refresh(metadata)

        last_synced_timestamp = await _get_last_synced_timestamp(db, senior_user_id, service_name)
        stories_to_sync = await _fetch_stories(db, story_creator_ids, service_name, full_resync=full_resync)
        
        # Filter stories based on last_synced_timestamp if not a full resync
        if not full_resync:
            stories_to_sync = [s for s in stories_to_sync if s['updated_at'].replace(tzinfo=timezone.utc) > last_synced_timestamp]

        async with httpx.AsyncClient() as client:
            synced_docs_list: List[Dict[str, Any]] = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
            synced_docs_map = {doc["story_id"]: doc["dify_document_id"] for doc in synced_docs_list}

            latest_timestamp = last_synced_timestamp
            
            if stories_to_sync:
                for story in stories_to_sync:
                    dify_data = _transform_story_to_dify_format(story, senior_user_id, service_name)
                    file_id = await upload_file_and_get_id(client, story.get("user_id"), dify_data)
                    if file_id:
                        document_id = await create_document_from_file(client, dataset_id, file_id, dify_data)
                        if document_id:
                            synced_docs_map[story.get("id")] = document_id
                    
                    story_updated_at = story.get("updated_at", datetime.min)
                    if story_updated_at.tzinfo is None:
                        story_updated_at = story_updated_at.replace(tzinfo=timezone.utc)
                    if story_updated_at > latest_timestamp:
                        latest_timestamp = story_updated_at
                
                _update_last_synced_timestamp(db, metadata, latest_timestamp)
                logger.info(f"Story upload process completed for senior_user_id {senior_user_id}.")
            else:
                logger.info(f"No new stories to sync for senior_user_id {senior_user_id}.")

            if not full_resync:
                current_story_ids = set(await _fetch_all_story_ids_for_user(story_creator_ids))
                deleted_story_ids = set(synced_docs_map.keys()) - current_story_ids
                
                if deleted_story_ids:
                    logger.info(f"Found {len(deleted_story_ids)} deleted stories for senior_user_id {senior_user_id}. Deleting from Dify...")
                    delete_tasks = []
                    for story_id in deleted_story_ids:
                        doc_id_to_delete = synced_docs_map.pop(story_id, None)
                        if doc_id_to_delete:
                            delete_tasks.append(delete_document_from_dify(client, dataset_id, doc_id_to_delete))
                    await asyncio.gather(*delete_tasks)
                    logger.info(f"Deletion process completed for senior_user_id {senior_user_id}.")
                else:
                    logger.info(f"No stories to delete for senior_user_id {senior_user_id}.")

        updated_synced_docs_list = [{"story_id": k, "dify_document_id": v} for k, v in synced_docs_map.items()]
        metadata.synced_story_ids = json.dumps(updated_synced_docs_list)
        db.commit()
        logger.info(f"Synced document map updated for senior_user_id {senior_user_id}.")
        logger.info(f"Full story synchronization process completed for senior_user_id: {senior_user_id}.")
    finally:
        if db_session:
            db_session.close()
