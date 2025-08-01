from sqlalchemy import Column, Integer, String, Text, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# story-sequencer의 Base와 동일한 역할을 하는 Base 생성
StorySequencerBase = declarative_base()

class Story(StorySequencerBase):
    __tablename__ = "stories"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    image_url = Column(String(512), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    segments = relationship("StorySegment", back_populates="story", cascade="all, delete-orphan")

class StorySegment(StorySequencerBase):
    __tablename__ = "story_segments"

    id = Column(Integer, primary_key=True, index=True)
    story_id = Column(Integer, ForeignKey("stories.id", ondelete="CASCADE"), nullable=False)
    order = Column(Integer, nullable=False)
    segment_text = Column(Text, nullable=False)

    story = relationship("Story", back_populates="segments")
