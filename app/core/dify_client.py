import logging
from typing import Optional, Dict, Any, List
import httpx
import asyncio
from sqlalchemy.orm import Session
from app.config.config import Config
from app.models.sync_metadata import SyncMetadata
import json

logger = logging.getLogger(__name__)

async def get_dify_metadata_fields(client: httpx.AsyncClient, dataset_id: str) -> Optional[Dict[str, str]]:
    access_token = await get_dify_access_token(client)
    if not access_token:
        return None

    url = f"{Config.DIFY_API_URL}/console/api/datasets/{dataset_id}" # Changed endpoint
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = await client.get(url, headers=headers, timeout=10.0)
        response.raise_for_status()
        result = response.json()
        
        metadata_fields = {}
        # Check for 'doc_metadata' key in the response
        if 'doc_metadata' in result and isinstance(result['doc_metadata'], list):
            for field in result['doc_metadata']:
                if 'name' in field and 'id' in field:
                    metadata_fields[field["name"]] = field["id"]
        else:
            logger.warning(f"No 'doc_metadata' found or it's not a list in Dify dataset response for {dataset_id}. Response: {result}")
            return None

        logger.info(f"Successfully obtained Dify metadata fields for dataset {dataset_id}.")
        return metadata_fields
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error obtaining Dify metadata fields: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Error obtaining Dify metadata fields: {e}")
        return None

async def get_dify_access_token(client: httpx.AsyncClient) -> Optional[str]:
    if not Config.DIFY_API_URL:
        logger.warning("DIFY_API_URL is not set. Cannot obtain Dify access token.")
        return None
    
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

async def upload_file_and_get_id(client: httpx.AsyncClient, user_id: int, data: Dict[str, Any]) -> Optional[str]:
    access_token = await get_dify_access_token(client)
    if not access_token:
        return None
    
    url = f"{Config.DIFY_API_URL}/console/api/files/upload"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        file_content = data["text"].encode('utf-8')
        file_name = data["name"] if data["name"].endswith(".txt") else data["name"] + ".txt"
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
                    "file_info_list": {"file_ids": [file_id]}
                }
            },
            "indexing_technique": data.get("indexing_technique", "high_quality"),
            "process_rule": {"mode": "automatic"},
            "doc_form": "text_model",
            "doc_language": "English",
            "retrieval_model": {
                "provider": "openai",
                "model_name": "text-embedding-ada-002",
                "search_method": "semantic_search",
                "reranking_enable": False,
                "top_k": 2,
                "score_threshold_enabled": False
            },
            "segmentation": {
                "separator": "\n",
                "max_tokens": 500
            }
            # "doc_metadata": data.get("meta", {}) # 문서 생성 시 메타데이터 직접 포함 불가
        }
        
        response = await client.post(url, json=payload, headers=headers, timeout=60.0)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Raw Dify document creation response for story_id {data['meta']['story_id']}: {result}")
        
        document_id = result.get('id')
        if not document_id and 'document' in result and isinstance(result['document'], dict):
             document_id = result['document'].get('id')

        if not document_id and 'documents' in result and isinstance(result['documents'], list) and len(result['documents']) > 0:
            first_document = result['documents'][0]
            if isinstance(first_document, dict):
                document_id = first_document.get('id')

        logger.info(f"Document ID extracted for story_id {data['meta']['story_id']}: {document_id}")
        return document_id
        
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error creating document for story_id {data['meta']['story_id']}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"Error creating document for story_id {data['meta']['story_id']}: {e}")
    return None

async def update_document_metadata(client: httpx.AsyncClient, dataset_id: str, document_id: str, metadata: Dict[str, Any]) -> bool:
    access_token = await get_dify_access_token(client)
    if not access_token:
        return False

    metadata_fields_map = await get_dify_metadata_fields(client, dataset_id)
    if not metadata_fields_map:
        logger.error(f"Failed to retrieve metadata fields map for dataset {dataset_id}. Cannot update metadata.")
        return False
    
    url = f"{Config.DIFY_API_URL}/console/api/datasets/{dataset_id}/documents/metadata"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    try:
        operation_data = []
        metadata_list = []
        for key, value in metadata.items():
            field_id = metadata_fields_map.get(key)
            if field_id:
                metadata_list.append({"id": field_id, "name": key, "value": value})
            else:
                logger.warning(f"Metadata field '{key}' not found in Dify dataset {dataset_id}. Skipping.")

        if not metadata_list:
            logger.info(f"No valid metadata fields to update for document {document_id}.")
            return True # No metadata to update, consider it successful

        operation_data.append({"document_id": document_id, "metadata_list": metadata_list})

        payload = {"operation_data": operation_data}

        response = await client.post(url, json=payload, headers=headers, timeout=60.0)
        response.raise_for_status()
        logger.info(f"Successfully updated metadata for document {document_id} in dataset {dataset_id}.")
        return True
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error updating metadata for document {document_id}: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"Error updating metadata for document {document_id}: {e}")
        return False

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
    return None

async def get_user_llm_metadata(db: Session, guardian_user_id: int) -> Dict[str, Any]:
    from app.core.user_service_client import get_senior_id_from_guardian_id
    
    senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
    if senior_user_id is None:
        logger.warning(f"No senior user found for guardian_user_id: {guardian_user_id}. Returning empty LLM metadata.")
        return {"user_id": guardian_user_id, "senior_user_id": None, "dify_dataset_id": None, "last_synced_timestamp": None, "synced_stories_count": 0}

    service_name = "story-sequencer"
    metadata = db.query(SyncMetadata).filter_by(user_id=senior_user_id, service_name=service_name).first()

    if not metadata:
        logger.warning(f"No sync metadata found for senior_user_id: {senior_user_id}. Returning empty LLM metadata.")
        return {"user_id": guardian_user_id, "senior_user_id": senior_user_id, "dify_dataset_id": Config.DIFY_DATASET_ID, "last_synced_timestamp": None, "synced_stories_count": 0}

    synced_docs_list = json.loads(metadata.synced_story_ids) if metadata.synced_story_ids else []
    
    return {
        "user_id": guardian_user_id,
        "senior_user_id": senior_user_id,
        "dify_dataset_id": Config.DIFY_DATASET_ID,
        "last_synced_timestamp": metadata.last_synced_timestamp.isoformat() if metadata.last_synced_timestamp else None,
        "synced_stories_count": len(synced_docs_list),
        "synced_story_ids": [doc["story_id"] for doc in synced_docs_list]
    }

async def get_user_llm_context(db: Session, guardian_user_id: int, query: str, top_k: int = 3) -> List[str]:
    from app.core.user_service_client import get_senior_id_from_guardian_id
    
    senior_user_id = await get_senior_id_from_guardian_id(guardian_user_id)
    if senior_user_id is None:
        logger.warning(f"No senior user found for guardian_user_id: {guardian_user_id}. Cannot retrieve LLM context.")
        return []

    dataset_id = Config.DIFY_DATASET_ID
    if not dataset_id:
        logger.error("DIFY_DATASET_ID is not configured. Cannot retrieve LLM context.")
        return []
    
    async with httpx.AsyncClient() as client:
        access_token = await get_dify_access_token(client)
        if not access_token:
            return []

        workflow_id = "YOUR_WORKFLOW_ID" 
        workflow_url = f"{Config.DIFY_API_URL}/v1/workflows/{workflow_id}/run"
        
        headers = {"Authorization": f"Bearer {Config.DIFY_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "inputs": {
                "sys_user_id": str(senior_user_id),
                "llm_prompt": query
            },
            "response_mode": "streaming", 
            "user": f"user_{guardian_user_id}"
        }

        try:
            response = await client.post(workflow_url, json=payload, headers=headers, timeout=180.0)
            response.raise_for_status()
            result = response.json()
            
            llm_output = result.get("data", {}).get("outputs", {}).get("llm_output", "")
            
            logger.info(f"Successfully retrieved LLM context for senior_user_id {senior_user_id} with query '{query}'.")
            return [llm_output] if llm_output else []
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error running Dify workflow for senior_user_id {senior_user_id}: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error running Dify workflow for senior_user_id {senior_user_id}: {e}")
            return []
