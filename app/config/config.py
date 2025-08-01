import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
    STORY_SEQUENCER_SERVICE_URL = os.getenv("STORY_SEQUENCER_SERVICE_URL", "http://story-sequencer:8011")
    DIFY_API_KEY = os.getenv("DIFY_API_KEY")
    DIFY_API_URL = os.getenv("DIFY_API_URL")
    DATABASE_URL = os.getenv("DATABASE_URL")
    # STORY_SEQUENCER_DATABASE_URL = os.getenv("STORY_SEQUENCER_DATABASE_URL") # 제거
    STORY_SEQUENCER_SECRET_KEY = os.getenv("SECRET_KEY") # story-sequencer의 SECRET_KEY
    STORY_SEQUENCER_ALGORITHM = "HS256"
    MOCK_USER_ID = 1 # 초기 개발을 위한 하드코딩된 사용자 ID
