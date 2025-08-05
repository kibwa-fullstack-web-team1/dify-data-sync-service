import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_story_sequencer_db, get_db as get_dify_db # get_db를 get_dify_db로 별칭 변경
from app.models.story_sequencer_models import Story
from typing import Optional, Dict, Any, List
import httpx
import asyncio

from app.core.user_service_client import get_senior_id_from_guardian_id, get_guardian_ids_for_senior
from app.core.dify_client import (
    get_dify_access_token,
    create_dify_dataset,
    get_or_create_dataset_id,
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

async def _fetch_stories(db: Session, user_id: int, service_name: str, full_resync: bool = False) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        if full_resync:
            logger.info(f"Fetching all stories for user_id: {user_id} for full resync.")
            query = story_sequencer_db.query(Story).filter(Story.user_id == user_id)
        else:
            last_synced = await _get_last_synced_timestamp(db, user_id, service_name)
            logger.info(f"Fetching new stories since {last_synced} for user_id: {user_id}.")
            query = story_sequencer_db.query(Story).filter(
                Story.user_id == user_id,
                Story.updated_at >= (last_synced.replace(tzinfo=None) if last_synced.tzinfo else last_synced)
            )
        stories = query.all()
        return [story.__dict__ for story in stories]
    finally:
        story_sequencer_db.close()

async def _fetch_all_story_ids_for_user(user_id: int) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        query = story_sequencer_db.query(Story.id).filter(Story.user_id == user_id)
        return [story_id[0] for story_id in query.all()]
    finally:
        story_sequencer_db.close()

def _transform_story_to_dify_format(story: dict) -> dict:
    created_at_iso = story.get("created_at").isoformat() if story.get("created_at") else None
    updated_at_iso = story.get("updated_at").isoformat() if story.get("updated_at") else None
    return {
        "text": story.get("content", ""),
        "name": story.get("title", f"story_{story.get('id')}"),
        "meta": {
            "user_id": story.get("user_id"), "story_id": story.get("id"),
            "title": story.get("title", ""), "created_at": created_at_iso,
            "updated_at": updated_at_iso, "source": "story-sequencer"
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

        logger.info(f"Starting story synchronization for guardian_user_id: {guardian_user_id} (senior_user_id: {senior_user_id})...")
        service_name = "story-sequencer"

        async with httpx.AsyncClient() as client:
            dataset_id = await get_or_create_dataset_id(db, client, senior_user_id, service_name)
            if not dataset_id:
                logger.error(f"Failed to get or create a dataset_id for senior_user_id: {senior_user_id}. Aborting sync.")
                return

            metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()
            if not metadata:
                metadata = SyncMetadata(user_id=senior_user_id, service_name=service_name, dify_dataset_id=dataset_id, synced_story_ids=json.dumps([]))
                db.add(metadata)
                db.commit()
                db.refresh(metadata)

            synced_docs_list: List[Dict[str, Any]] = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
            synced_docs_map = {doc["story_id"]: doc["dify_document_id"] for doc in synced_docs_list}

            stories_to_sync = await _fetch_stories(db, guardian_user_id, service_name, full_resync=full_resync)
            latest_timestamp = await _get_last_synced_timestamp(db, senior_user_id, service_name)
            
            if stories_to_sync:
                for story in stories_to_sync:
                    dify_data = _transform_story_to_dify_format(story)
                    file_id = await upload_file_and_get_id(client, senior_user_id, dify_data)
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
                logger.info(f"No new stories to sync for guardian_user_id {guardian_user_id}.")

            if not full_resync:
                # Fetch all story IDs for the guardian, as stories are linked to the guardian in story-sequencer
                current_story_ids = set(await _fetch_all_story_ids_for_user(guardian_user_id))
                deleted_story_ids = set(synced_docs_map.keys()) - current_story_ids
                
                if deleted_story_ids:
                    logger.info(f"Found {len(deleted_story_ids)} deleted stories for guardian_user_id {guardian_user_id}. Deleting from Dify...")
                    delete_tasks = []
                    for story_id in deleted_story_ids:
                        doc_id_to_delete = synced_docs_map.pop(story_id, None)
                        if doc_id_to_delete:
                            delete_tasks.append(delete_document_from_dify(client, dataset_id, doc_id_to_delete))
                    await asyncio.gather(*delete_tasks)
                    logger.info(f"Deletion process completed for senior_user_id {senior_user_id}.")
                else:
                    logger.info(f"No stories to delete for guardian_user_id {guardian_user_id}.")

        updated_synced_docs_list = [{"story_id": k, "dify_document_id": v} for k, v in synced_docs_map.items()]
        metadata.synced_story_ids = json.dumps(updated_synced_docs_list)
        db.commit()
        logger.info(f"Synced document map updated for senior_user_id {senior_user_id}.")
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

        metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()
        if not (metadata and metadata.dify_dataset_id):
            logger.warning(f"No dataset found for senior_user_id {senior_user_id}. Nothing to clear.")
            return False

        dataset_id = metadata.dify_dataset_id
        synced_docs_list = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
        document_ids = [doc["dify_document_id"] for doc in synced_docs_list]

        async with httpx.AsyncClient() as client:
            delete_tasks = [delete_document_from_dify(client, dataset_id, doc_id) for doc_id in document_ids]
            await asyncio.gather(*delete_tasks)
        
        metadata.synced_story_ids = json.dumps([])
        metadata.last_synced_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        db.commit()
        logger.info(f"Successfully cleared dataset {dataset_id} for senior_user_id {senior_user_id}.")
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
        
        # 시니어와 연결된 모든 보호자 ID를 가져옵니다.
        guardian_ids = await get_guardian_ids_for_senior(senior_user_id)
        
        if not guardian_ids:
            logger.warning(f"No guardians found for senior_user_id: {senior_user_id}. Skipping synchronization.")
            return

        # 각 보호자 ID에 대해 동기화를 트리거합니다.
        # Dify 데이터셋은 시니어 단위로 관리되므로, 어떤 보호자를 통해 호출해도 동일한 시니어의 데이터셋에 동기화됩니다.
        for guardian_id in guardian_ids:
            logger.info(f"Triggering sync_stories for guardian_user_id: {guardian_id} (senior_user_id: {senior_user_id})...")
            await sync_stories(db, guardian_id, full_resync=full_resync)
        
        logger.info(f"Story synchronization for senior_user_id: {senior_user_id} completed.")
    finally:
        if db_session:
            db_session.close()
