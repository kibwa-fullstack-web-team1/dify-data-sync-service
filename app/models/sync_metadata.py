from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from app.utils.db import Base

class SyncMetadata(Base):
    __tablename__ = "sync_metadata"

    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String, unique=True, index=True, nullable=False)
    last_synced_timestamp = Column(DateTime(timezone=True), nullable=True)
    last_synced_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
