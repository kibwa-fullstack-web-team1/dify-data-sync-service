from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config.config import Config

SQLALCHEMY_DATABASE_URL = Config.DATABASE_URL

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# story-sequencer DB 연결을 위한 새로운 엔진 및 세션 (기존 DATABASE_URL 재활용)
story_sequencer_engine = create_engine(SQLALCHEMY_DATABASE_URL)
StorySequencerSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=story_sequencer_engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_story_sequencer_db():
    db = StorySequencerSessionLocal()
    try:
        yield db
    finally:
        db.close()
