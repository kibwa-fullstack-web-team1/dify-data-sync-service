import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_db as get_dify_db
from typing import Optional, Dict, Any, List
import httpx
import asyncio

from app.core.user_service_client import get_senior_id_from_guardian_id, get_guardian_ids_for_senior
from app.core.dify_client import (
    upload_file_and_get_id,
    create_document_from_file,
    delete_document_from_dify,
    get_user_llm_metadata,
    get_user_llm_context,
    update_document_metadata
)
from app.helper.story_sync_helper import (
    get_last_synced_timestamp,
    update_last_synced_timestamp,
    fetch_stories,
    fetch_all_story_ids_for_user,
    sanitize_title_for_filename,
    transform_story_to_dify_format
)

logger = logging.getLogger(__name__)

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

        last_synced_timestamp = await get_last_synced_timestamp(db, senior_user_id, service_name)
        stories_to_sync = await fetch_stories(db, story_creator_ids, service_name, full_resync=full_resync)
        
        if not full_resync:
            stories_to_sync = [s for s in stories_to_sync if s['updated_at'].replace(tzinfo=timezone.utc) > last_synced_timestamp]

        async with httpx.AsyncClient() as client:
            synced_docs_list: List[Dict[str, Any]] = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
            synced_docs_map = {doc["story_id"]: doc["dify_document_id"] for doc in synced_docs_list}

            latest_timestamp = last_synced_timestamp
            
            if stories_to_sync:
                for story in stories_to_sync:
                    dify_data = transform_story_to_dify_format(story, senior_user_id, service_name)
                    file_id = await upload_file_and_get_id(client, story.get("user_id"), dify_data)
                    if file_id:
                        document_id = await create_document_from_file(client, dataset_id, file_id, dify_data)
                        if document_id:
                            # 문서 생성 후 메타데이터 업데이트
                            await update_document_metadata(client, dataset_id, document_id, dify_data["meta"])
                            synced_docs_map[story.get("id")] = document_id
                    
                    story_updated_at = story.get("updated_at", datetime.min)
                    if story_updated_at.tzinfo is None:
                        story_updated_at = story_updated_at.replace(tzinfo=timezone.utc)
                    if story_updated_at > latest_timestamp:
                        latest_timestamp = story_updated_at
                
                update_last_synced_timestamp(db, metadata, latest_timestamp)
                logger.info(f"Story upload process completed for senior_user_id {senior_user_id}.")
            else:
                logger.info(f"No new stories to sync for senior_user_id {senior_user_id}.")

            if not full_resync:
                current_story_ids = set(await fetch_all_story_ids_for_user(story_creator_ids))
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