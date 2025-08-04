import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_story_sequencer_db
from app.models.story_sequencer_models import Story
from typing import Optional, Dict, Any, List
import httpx
import asyncio

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

# --- Dify API Interaction ---

async def _get_dify_access_token(client: httpx.AsyncClient) -> Optional[str]:
    url = f"{Config.DIFY_API_URL}/console/api/login"
    payload = {
        "email": Config.DIFY_ADMIN_EMAIL,
        "password": Config.DIFY_ADMIN_PASSWORD
    }
    try:
        response = await client.post(url, json=payload, timeout=10.0)
        response.raise_for_status()
        result = response.json()
        access_token = result.get("data", {}).get("access_token")
        if access_token:
            logger.info("Successfully obtained Dify access token.")
            return access_token
        else:
            logger.error(f"Dify login successful but no access_token found in response: {result}")
            return None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error obtaining Dify access token: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Error obtaining Dify access token: {e}")
        return None

async def _create_dify_dataset(client: httpx.AsyncClient, user_id: int) -> Optional[str]:
    access_token = await _get_dify_access_token(client)
    if not access_token:
        return None
    url = f"{Config.DIFY_API_URL}/console/api/datasets"
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {"name": f"user_{user_id}_stories"}
    try:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        dataset_id = result.get("id")
        logger.info(f"Successfully created Dify dataset for user_id {user_id}. New dataset_id: {dataset_id}")
        return dataset_id
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error creating dataset for user_id {user_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error creating dataset for user_id {user_id}: {e}")
    return None

async def _get_or_create_dataset_id(db: Session, client: httpx.AsyncClient, user_id: int, service_name: str) -> Optional[str]:
    metadata = db.query(SyncMetadata).filter_by(user_id=user_id, service_name=service_name).first()
    if metadata and metadata.dify_dataset_id:
        return metadata.dify_dataset_id

    new_dataset_id = await _create_dify_dataset(client, user_id)
    if new_dataset_id:
        if metadata:
            metadata.dify_dataset_id = new_dataset_id
        else:
            metadata = SyncMetadata(user_id=user_id, service_name=service_name, dify_dataset_id=new_dataset_id)
            db.add(metadata)
        db.commit()
        return new_dataset_id
    return None

async def _upload_file_and_get_id(client: httpx.AsyncClient, user_id: int, data: Dict[str, Any]) -> Optional[str]:
    access_token = await _get_dify_access_token(client)
    if not access_token:
        return None
    url = f"{Config.DIFY_API_URL}/console/api/files/upload"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        file_content = data["text"].encode('utf-8')
        file_name = data["name"] + ".txt"
        files = {'file': (file_name, file_content, 'text/plain')}
        form_data = {'user': str(user_id)}
        response = await client.post(url, files=files, data=form_data, headers=headers, timeout=60.0)
        response.raise_for_status()
        result = response.json()
        file_id = result.get('id')
        logger.info(f"Successfully uploaded file for story_id: {data['meta']['story_id']}. Dify file_id: {file_id}")
        return file_id
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error uploading file for story_id {data['meta']['story_id']}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error uploading file for story_id {data['meta']['story_id']}: {e}")
    return None

async def _create_document_from_file(client: httpx.AsyncClient, dataset_id: str, file_id: str, data: Dict[str, Any]) -> Optional[str]:
    access_token = await _get_dify_access_token(client)
    if not access_token:
        return None
    url = f"{Config.DIFY_API_URL}/console/api/datasets/{dataset_id}/documents"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        payload = {
            "name": data["name"],
            "data_source": {
                "type": "upload_file",
                "info_list": {
                    "data_source_type": "upload_file",
                    "file_info_list": {
                        "file_ids": [file_id]
                    }
                }
            },
            "indexing_technique": data["indexing_technique"],
            "process_rule": {
                "mode": "automatic"
            }
        }
        response = await client.post(url, json=payload, headers=headers, timeout=60.0)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Dify document creation response for story_id {data['meta']['story_id']}: {result}")
        document_id = result[0].get('id') if isinstance(result, list) and result else result.get('id')
        logger.info(f"Successfully created document for story_id: {data['meta']['story_id']}. Dify document_id: {document_id}")
        return document_id
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error creating document for story_id {data['meta']['story_id']}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error creating document for story_id {data['meta']['story_id']}: {e}")
    return None

async def _delete_document_from_dify(client: httpx.AsyncClient, dataset_id: str, document_id: str):
    access_token = await _get_dify_access_token(client)
    if not access_token:
        return
    url = f"{Config.DIFY_API_URL}/console/api/datasets/{dataset_id}/documents/{document_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = await client.delete(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully deleted document {document_id} from dataset {dataset_id}")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error deleting document {document_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error deleting document {document_id}: {e}")

# --- Main Service Logic ---

async def sync_stories(db: Session, user_id: Optional[int] = None, full_resync: bool = False):
    if user_id is None:
        logger.warning("sync_stories called without user_id. Skipping.")
        return

    logger.info(f"Starting story synchronization for user_id: {user_id}...")
    service_name = "story-sequencer"

    async with httpx.AsyncClient() as client:
        dataset_id = await _get_or_create_dataset_id(db, client, user_id, service_name)
        if not dataset_id:
            logger.error(f"Failed to get or create a dataset_id for user_id: {user_id}. Aborting sync.")
            return

        metadata = db.query(SyncMetadata).filter_by(user_id=user_id, service_name=service_name).first()
        synced_docs_list: List[Dict[str, Any]] = json.loads(metadata.synced_story_ids) if metadata and metadata.synced_story_ids else []
        synced_docs_map = {doc["story_id"]: doc["dify_document_id"] for doc in synced_docs_list}

        stories_to_sync = await _fetch_stories(db, user_id, service_name, full_resync=full_resync)
        latest_timestamp = await _get_last_synced_timestamp(db, user_id, service_name)
        
        if stories_to_sync:
            for story in stories_to_sync:
                dify_data = _transform_story_to_dify_format(story)
                file_id = await _upload_file_and_get_id(client, user_id, dify_data)
                if file_id:
                    document_id = await _create_document_from_file(client, dataset_id, file_id, dify_data)
                    if document_id:
                        synced_docs_map[story.get("id")] = document_id
                
                story_updated_at = story.get("updated_at", datetime.min)
                if story_updated_at.tzinfo is None:
                    story_updated_at = story_updated_at.replace(tzinfo=timezone.utc)
                if story_updated_at > latest_timestamp:
                    latest_timestamp = story_updated_at
            
            _update_last_synced_timestamp(db, metadata, latest_timestamp)
            logger.info(f"Story upload process completed for user_id {user_id}.")
        else:
            logger.info(f"No new stories to sync for user_id {user_id}.")

        if not full_resync:
            current_story_ids = set(await _fetch_all_story_ids_for_user(user_id))
            deleted_story_ids = set(synced_docs_map.keys()) - current_story_ids
            
            if deleted_story_ids:
                logger.info(f"Found {len(deleted_story_ids)} deleted stories for user_id {user_id}. Deleting from Dify...")
                delete_tasks = []
                for story_id in deleted_story_ids:
                    doc_id_to_delete = synced_docs_map.pop(story_id, None)
                    if doc_id_to_delete:
                        delete_tasks.append(_delete_document_from_dify(client, dataset_id, doc_id_to_delete))
                await asyncio.gather(*delete_tasks)
                logger.info(f"Deletion process completed for user_id {user_id}.")
            else:
                logger.info(f"No stories to delete for user_id {user_id}.")

    updated_synced_docs_list = [{"story_id": k, "dify_document_id": v} for k, v in synced_docs_map.items()]
    metadata.synced_story_ids = json.dumps(updated_synced_docs_list)
    db.commit()
    logger.info(f"Synced document map updated for user_id {user_id}.")
    logger.info(f"Full story synchronization process completed for user_id: {user_id}.")

async def clear_dataset(db: Session, user_id: int) -> bool:
    logger.info(f"Clearing dataset for user_id: {user_id}")
    service_name = "story-sequencer"
    metadata = db.query(SyncMetadata).filter_by(user_id=user_id, service_name=service_name).first()
    if not (metadata and metadata.dify_dataset_id):
        logger.warning(f"No dataset found for user_id {user_id}. Nothing to clear.")
        return False

    dataset_id = metadata.dify_dataset_id
    synced_docs_list = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
    document_ids = [doc["dify_document_id"] for doc in synced_docs_list]

    async with httpx.AsyncClient() as client:
        delete_tasks = [_delete_document_from_dify(client, dataset_id, doc_id) for doc_id in document_ids]
        await asyncio.gather(*delete_tasks)
    
    metadata.synced_story_ids = json.dumps([])
    metadata.last_synced_timestamp = datetime.min.replace(tzinfo=timezone.utc)
    db.commit()
    logger.info(f"Successfully cleared dataset {dataset_id} for user_id {user_id}.")
    return True

async def rebuild_dataset(db: Session, user_id: int):
    logger.info(f"Rebuilding dataset for user_id: {user_id}")
    cleared = await clear_dataset(db, user_id)
    if cleared:
        await sync_stories(db, user_id, full_resync=True)
        logger.info(f"Dataset rebuild completed for user_id: {user_id}.")
    else:
        logger.error(f"Could not clear dataset for user_id {user_id}, rebuild aborted.")
