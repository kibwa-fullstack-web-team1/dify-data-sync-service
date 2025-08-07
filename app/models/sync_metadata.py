from sqlalchemy import Column, Integer, String, DateTime, func, UniqueConstraint
from app.utils.db import Base

class SyncMetadata(Base):
    __tablename__ = "sync_metadata"

    id = Column(Integer, primary_key=True, index=True)
    # user_id와 service_name 조합이 고유하도록 설정
    user_id = Column(Integer, index=True, nullable=False)
    service_name = Column(String, index=True, nullable=False)
    last_synced_timestamp = Column(DateTime(timezone=True), nullable=True)
    synced_story_ids = Column(String, nullable=True) # 기존 JSON 문자열 저장 방식 유지
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    # user_id와 service_name이 항상 쌍으로 유일해야 함을 보장
    __table_args__ = (UniqueConstraint('user_id', 'service_name', name='_user_service_uc'),)