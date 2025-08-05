import logging
from typing import Optional, Dict, Any, List
import httpx
import asyncio
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json

logger = logging.getLogger(__name__)

async def get_dify_access_token(client: httpx.AsyncClient) -> Optional[str]:
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

async def create_dify_dataset(client: httpx.AsyncClient, senior_user_id: int) -> Optional[str]:
    access_token = await get_dify_access_token(client)
    if not access_token:
        return None
    url = f"{Config.DIFY_API_URL}/console/api/datasets"
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {"name": f"senior_user_{senior_user_id}_stories"}
    try:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        dataset_id = result.get("id")
        logger.info(f"Successfully created Dify dataset for senior_user_id {senior_user_id}. New dataset_id: {dataset_id}")
        return dataset_id
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error creating dataset for senior_user_id {senior_user_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error creating dataset for senior_user_id {senior_user_id}: {e}")
    return None

async def get_or_create_dataset_id(db: Session, client: httpx.AsyncClient, senior_user_id: int, service_name: str) -> Optional[str]:
    metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()
    if metadata and metadata.dify_dataset_id:
        return metadata.dify_dataset_id

    new_dataset_id = await create_dify_dataset(client, senior_user_id)
    if new_dataset_id:
        if metadata:
            metadata.dify_dataset_id = new_dataset_id
        else:
            metadata = SyncMetadata(user_id=senior_user_id, service_name=service_name, dify_dataset_id=new_dataset_id)
            db.add(metadata)
        db.commit()
        return new_dataset_id
    return None

async def upload_file_and_get_id(client: httpx.AsyncClient, user_id: int, data: Dict[str, Any]) -> Optional[str]:
    access_token = await get_dify_access_token(client)
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

async def create_document_from_file(client: httpx.AsyncClient, dataset_id: str, file_id: str, data: Dict[str, Any]) -> Optional[str]:
    access_token = await get_dify_access_token(client)
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
        logger.info(f"Raw Dify document creation response for story_id {data['meta']['story_id']}: {result}")
        logger.info(f"Raw Dify document creation response for story_id {data['meta']['story_id']}: {result}")
        document_id = None
        if result and 'documents' in result and isinstance(result['documents'], list) and len(result['documents']) > 0:
            first_document = result['documents'][0]
            logger.info(f"First document in Dify response for story_id {data['meta']['story_id']}: {first_document}")
            if isinstance(first_document, dict):
                document_id = first_document.get('id')
                logger.info(f"Directly extracted document_id: {document_id}")
            else:
                logger.warning(f"First document in Dify response for story_id {data['meta']['story_id']} is not a dictionary: {first_document}")
        else:
            logger.warning(f"Dify document creation response for story_id {data['meta']['story_id']} did not contain expected 'documents' structure: {result}")
        logger.info(f"Document ID extracted for story_id {data['meta']['story_id']}: {document_id}")
        return document_id
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error creating document for story_id {data['meta']['story_id']}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error creating document for story_id {data['meta']['story_id']}: {e}")
    return None

async def delete_document_from_dify(client: httpx.AsyncClient, dataset_id: str, document_id: str):
    access_token = await get_dify_access_token(client)
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

async def get_user_llm_metadata(db: Session, guardian_user_id: int) -> Dict[str, Any]:
    """
    특정 사용자의 Dify 데이터셋에서 LLM 프롬프트 활용에 적합한 메타데이터를 조회합니다.
    현재는 Dify 데이터셋의 메타데이터를 직접 조회하는 API가 없으므로,
    SyncMetadata에 저장된 정보를 기반으로 구성합니다.
    향후 Dify API가 확장되면 해당 API를 직접 호출하도록 변경될 수 있습니다.
    """
    from app.core.user_service_client import get_senior_id_from_guardian_id
    senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
    if senior_user_id is None:
        logger.warning(f"No senior user found for guardian_user_id: {guardian_user_id}. Returning empty LLM metadata.")
        return {"user_id": guardian_user_id, "senior_user_id": None, "dify_dataset_id": None, "last_synced_timestamp": None, "synced_stories_count": 0}

    service_name = "story-sequencer"
    metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()

    if not metadata:
        logger.warning(f"No sync metadata found for senior_user_id: {senior_user_id}. Returning empty LLM metadata.")
        return {"user_id": guardian_user_id, "senior_user_id": senior_user_id, "dify_dataset_id": None, "last_synced_timestamp": None, "synced_stories_count": 0}

    synced_docs_list = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
    
    return {
        "user_id": guardian_user_id,
        "senior_user_id": senior_user_id,
        "dify_dataset_id": metadata.dify_dataset_id,
        "last_synced_timestamp": metadata.last_synced_timestamp.isoformat() if metadata.last_synced_timestamp else None,
        "synced_stories_count": len(synced_docs_list),
        "synced_story_ids": [doc["story_id"] for doc in synced_docs_list]
    }

async def get_user_llm_context(db: Session, guardian_user_id: int, query: str, top_k: int = 3) -> List[str]:
    """
    특정 사용자의 Dify 데이터셋에서 주어진 쿼리에 대한 관련 문서를 검색하여 LLM 컨텍스트를 제공합니다.
    이 함수는 Dify의 RAG(Retrieval Augmented Generation) 기능을 활용합니다.
    """
    from app.core.user_service_client import get_senior_id_from_guardian_id
    senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
    if senior_user_id is None:
        logger.warning(f"No senior user found for guardian_user_id: {guardian_user_id}. Cannot retrieve LLM context.")
        return []

    service_name = "story-sequencer"
    metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()

    if not metadata or not metadata.dify_dataset_id:
        logger.warning(f"No Dify dataset found for senior_user_id: {senior_user_id}. Cannot retrieve LLM context.")
        return []

    dataset_id = metadata.dify_dataset_id
    
    async with httpx.AsyncClient() as client:
        access_token = await get_dify_access_token(client)
        if not access_token:
            return []

        # Dify의 Dataset Query API를 호출하여 관련 문서 검색
        # Dify API 문서에 따라 정확한 엔드포인트와 페이로드 확인 필요
        # 현재 Dify Community Edition의 Dataset Query API는 /datasets/{dataset_id}/documents/search 로 보임
        # 하지만 실제 RAG를 위한 Query API는 /datasets/{dataset_id}/completion 또는 /datasets/{dataset_id}/retrieval 일 수 있음.
        # 여기서는 임시로 /datasets/{dataset_id}/documents/search 를 사용하고, 실제 문서 내용을 반환한다고 가정합니다.
        
        query_url = f"{Config.DIFY_API_URL}/console/api/datasets/{dataset_id}/documents/search"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {
            "query": query,
            "top_k": top_k,
            "user": str(senior_user_id) # Dify API에 사용자 ID를 전달하여 개인화된 검색을 유도
        }

        try:
            response = await client.post(query_url, json=payload, headers=headers, timeout=30.0)
            response.raise_for_status()
            result = response.json()
            
            # Dify API 응답 구조에 따라 관련 문서의 내용을 추출
            # 예시: result = {"data": [{"content": "...", "metadata": {...}}, ...]}
            context_texts = []
            if "data" in result and isinstance(result["data"], list):
                for item in result["data"]:
                    if "content" in item:
                        context_texts.append(item["content"])
            
            logger.info(f"Successfully retrieved {len(context_texts)} context texts for senior_user_id {senior_user_id} with query '{query}'.")
            return context_texts
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error querying Dify dataset for senior_user_id {senior_user_id}: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error querying Dify dataset for senior_user_id {senior_user_id}: {e}")
            return []
