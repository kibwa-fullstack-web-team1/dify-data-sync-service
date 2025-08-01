import httpx
import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata

logger = logging.getLogger(__name__)

class StorySyncService:
    def __init__(self, db: Session):
        self.db = db
        self.story_sequencer_url = Config.STORY_SEQUENCER_SERVICE_URL
        self.service_name = "story-sequencer"

    async def get_last_synced_timestamp(self) -> datetime:
        metadata = self.db.query(SyncMetadata).filter_by(service_name=self.service_name).first()
        if metadata and metadata.last_synced_timestamp:
            return metadata.last_synced_timestamp
        # 초기 동기화를 위해 매우 오래된 타임스탬프 반환
        return datetime.min

    def update_last_synced_timestamp(self, new_timestamp: datetime):
        metadata = self.db.query(SyncMetadata).filter_by(service_name=self.service_name).first()
        if metadata:
            metadata.last_synced_timestamp = new_timestamp
        else:
            self.db.add(SyncMetadata(service_name=self.service_name, last_synced_timestamp=new_timestamp))
        self.db.commit()

    async def fetch_new_stories(self) -> list:
        last_synced = await self.get_last_synced_timestamp()
        # story-sequencer API가 updated_after 파라미터를 지원한다고 가정
        # 만약 지원하지 않는다면, 모든 데이터를 가져와 필터링 로직을 추가해야 함
        params = {"updated_after": last_synced.isoformat()}
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.story_sequencer_url}/api/v0/stories/", params=params)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Error fetching stories: {e.response.status_code} - {e.response.text}")
                return []
            except httpx.RequestError as e:
                logger.error(f"Network error fetching stories: {e}")
                return []

    def transform_story_to_dify_format(self, story: dict) -> dict:
        # Dify Document Upload API 형식에 맞게 데이터 변환
        # user_id를 메타데이터로 포함
        return {
            "text": story.get("content", ""),
            "meta": {
                "user_id": story.get("user_id"),
                "story_id": story.get("id"),
                "title": story.get("title", ""),
                "created_at": story.get("created_at"),
                "updated_at": story.get("updated_at"),
                "source": self.service_name
            }
        }

    async def sync_stories(self):
        logger.info(f"Starting story synchronization for {self.service_name}...")
        new_stories = await self.fetch_new_stories()
        if not new_stories:
            logger.info("No new stories to sync.")
            return

        latest_timestamp = await self.get_last_synced_timestamp()
        for story in new_stories:
            # Dify 업로드 로직 (다음 태스크에서 구현 예정)
            dify_data = self.transform_story_to_dify_format(story)
            logger.info(f"Transformed story for Dify: {dify_data}")
            
            # 마지막 동기화 타임스탬프 업데이트
            story_updated_at = datetime.fromisoformat(story["updated_at"]) if "updated_at" in story else datetime.min
            if story_updated_at > latest_timestamp:
                latest_timestamp = story_updated_at

        self.update_last_synced_timestamp(latest_timestamp)
        logger.info(f"Story synchronization completed. Last synced timestamp updated to {latest_timestamp}")
