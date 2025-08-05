import logging
from typing import Optional, List, Dict, Any
import httpx
from app.config.config import Config

logger = logging.getLogger(__name__)

async def get_senior_id_from_guardian_id(guardian_id: int) -> Optional[int]:
    """보호자 ID로부터 연결된 시니어 사용자 ID를 조회합니다."""
    url = f"{Config.USER_SERVICE_URL}/users/guardian/{guardian_id}/seniors"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            seniors_data = response.json()
            
            if seniors_data and isinstance(seniors_data, list):
                # 첫 번째 시니어 사용자의 ID를 반환
                senior_id = seniors_data[0].get("id")
                if senior_id:
                    logger.info(f"Found senior_id {senior_id} for guardian_id {guardian_id}.")
                    return senior_id
                else:
                    logger.warning(f"Senior data found for guardian_id {guardian_id} but no ID in response: {seniors_data}")
                    return None
            else:
                logger.warning(f"No senior data found for guardian_id {guardian_id} in user-service response: {seniors_data}")
                return None
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching seniors for guardian_id {guardian_id}: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error fetching seniors for guardian_id {guardian_id}: {e}")
            return None