from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from app.utils.db import Base

class SyncMetadata(Base):
    __tablename__ = "sync_metadata"

    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String, unique=True, index=True, nullable=False)
    last_synced_timestamp = Column(DateTime(timezone=True), nullable=True)
    last_synced_id = Column(String, nullable=True)
    synced_story_ids = Column(String, nullable=True) # 동기화된 이야기 ID 목록을 JSON 문자열로 저장
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())