import logging
import re
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json
from app.utils.db import get_story_sequencer_db
from app.models.story_sequencer_models import Story
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

async def get_last_synced_timestamp(db: Session, user_id: int, service_name: str) -> datetime:
    metadata = db.query(SyncMetadata).filter_by(user_id=user_id, service_name=service_name).first()
    if metadata and metadata.last_synced_timestamp:
        return metadata.last_synced_timestamp
    return datetime.min.replace(tzinfo=timezone.utc)

def update_last_synced_timestamp(db: Session, metadata: SyncMetadata, new_timestamp: datetime):
    metadata.last_synced_timestamp = new_timestamp
    db.commit()

async def fetch_stories(db: Session, user_ids: List[int], service_name: str, full_resync: bool = False) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        if full_resync:
            logger.info(f"Fetching all stories for user_ids: {user_ids} for full resync.")
            query = story_sequencer_db.query(Story).filter(Story.user_id.in_(user_ids))
        else:
            latest_synced_time = datetime.min.replace(tzinfo=timezone.utc)
            if user_ids:
                pass

            logger.info(f"Fetching new stories for user_ids: {user_ids}.")
            query = story_sequencer_db.query(Story).filter(Story.user_id.in_(user_ids))

        stories = query.all()
        return [story.__dict__ for story in stories]
    finally:
        story_sequencer_db.close()

async def fetch_all_story_ids_for_user(user_ids: List[int]) -> list:
    story_sequencer_db = next(get_story_sequencer_db())
    try:
        query = story_sequencer_db.query(Story.id).filter(Story.user_id.in_(user_ids))
        return [story_id[0] for story_id in query.all()]
    finally:
        story_sequencer_db.close()

def sanitize_title_for_filename(title: str) -> str:
    if not title:
        return ""
    title = re.sub(r'[\\/:*?"<>|]', '', title)
    title = re.sub(r'\s+', '_', title)
    return title[:50]

def transform_story_to_dify_format(story: dict, senior_user_id: int, service_name: str) -> dict:
    author_user_id = story.get("user_id")
    story_id = story.get("id")
    title = story.get("title", "")
    sanitized_title = sanitize_title_for_filename(title)

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
        },
        "indexing_technique": "high_quality"
    }
