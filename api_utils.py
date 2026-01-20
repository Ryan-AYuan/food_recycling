import os
import logging
import aiohttp
import aiofiles
import asyncio
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
API_WORKFLOW_RUN_URL_PRO = os.getenv("API_WORKFLOW_RUN_URL_PRO")
API_FILE_UPLOAD_URL_PRO = os.getenv("API_FILE_UPLOAD_URL_PRO")

# --- 異步文件上傳 ---
async def _upload_single_file_async(session: aiohttp.ClientSession, local_file_path: str, file_type: str, file_content_type: str, api_key: str, user: str) -> Optional[str]:
    """異步上傳單個文件"""
    url = API_FILE_UPLOAD_URL_PRO
    headers = {'Authorization': f'Bearer {api_key}'}
    file_name = os.path.basename(local_file_path)
    
    data = aiohttp.FormData()
    data.add_field('user', user)
    data.add_field('type', file_type)
    
    try:
        async with aiofiles.open(local_file_path, 'rb') as f:
            file_content = await f.read()
            data.add_field('file', file_content, filename=file_name, content_type=file_content_type)
            
            async with session.post(url, headers=headers, data=data) as response:
                if response.status == 201:
                    result = await response.json()
                    file_id = result.get('id')
                    logger.info(f"文件 {file_name} 上傳成功，ID: {file_id}")
                    return file_id
                else:
                    error_text = await response.text()
                    logger.error(f"文件 {file_name} 上傳失敗: {response.status} - {error_text}")
                    return None
    except Exception as e:
        logger.error(f"上傳文件 {file_name} 過程中發生異常: {e}", exc_info=True)
        return None

async def upload_files_async(local_file_path_list: List[str], file_type: str, file_content_type: str, api_key: str, user: str) -> List[str]:
    """並發上傳文件列表"""
    if not local_file_path_list:
        return []
        
    async with aiohttp.ClientSession() as session:
        tasks = [
            _upload_single_file_async(session, path, file_type, file_content_type, api_key, user)
            for path in local_file_path_list
        ]
        results = await asyncio.gather(*tasks)
        # 過濾掉上傳失敗的 None 結果
        return [file_id for file_id in results if file_id is not None]

async def upload_file_pdf_pdf2content(local_file_path_list: List[str], api_key: str, user: str) -> List[str]:
    """異步上傳PDF文件列表至Dify"""
    return await upload_files_async(local_file_path_list, 'pdf', 'application/pdf', api_key, user)

async def upload_file_image_pdf2content(local_file_path_list: List[str], api_key: str, user: str) -> List[str]:
    """異步上傳圖片文件列表至Dify"""
    # 假設所有圖片都是png，如果不是，需要更複雜的邏輯來判斷mime type
    return await upload_files_async(local_file_path_list, 'png', 'image/png', api_key, user)


# --- 異步工作流執行 ---
async def _run_workflow_async(api_key: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """通用的異步工作流執行器"""
    workflow_url = API_WORKFLOW_RUN_URL_PRO
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(workflow_url, headers=headers, json=data) as response:
                    if response.status == 200:
                        logger.info("工作流執行成功")
                        return await response.json()
                    else:
                        error_text = await response.text()
                        logger.error(f"工作流執行失敗 (嘗試 {attempt + 1}): {response.status} - {error_text}")
        except Exception as e:
            logger.error(f"工作流執行異常 (嘗試 {attempt + 1}): {e}", exc_info=True)
        
        if attempt < max_retries - 1:
            await asyncio.sleep(2) # 重試前等待
            
    return {"error": "工作流執行失敗，已達最大重試次數"}


async def run_workflow_pdf_and_image_pdf2content(pdf_file_ids: List[str], image_file_ids: List[str], api_key: str, user: str, workflow_id: str = None) -> Dict[str, Any]:
    """異步執行工作流（PDF和圖片）"""
    pdf_list = [{"type": "document", "transfer_method": "local_file", "upload_file_id": file_id} for file_id in pdf_file_ids]
    image_list = [{"type": "image", "transfer_method": "local_file", "upload_file_id": file_id} for file_id in image_file_ids]
    
    data = {
        "inputs": {"file_pdf": pdf_list, "file_image": image_list},
        "response_mode": "blocking",
        "user": user
    }
    if workflow_id:
        data["workflow_id"] = workflow_id
        
    return await _run_workflow_async(api_key, data)

async def run_workflow_pdf_pdf2content(pdf_file_ids: List[str], api_key: str, user: str, workflow_id: str = None) -> Dict[str, Any]:
    """異步執行工作流（僅PDF）"""
    pdf_list = [{"type": "document", "transfer_method": "local_file", "upload_file_id": file_id} for file_id in pdf_file_ids]
    
    data = {
        "inputs": {"file_pdf": pdf_list},
        "response_mode": "blocking",
        "user": user
    }
    if workflow_id:
        data["workflow_id"] = workflow_id
        
    return await _run_workflow_async(api_key, data)

async def run_workflow_foodsafety(globalId_content_dict_list: List[Dict], globalId_title_dict_list: List[Dict], api_key: str, user: str, workflow_id: str = None) -> Dict[str, Any]:
    """異步執行foodsafety工作流"""
    data = {
        "inputs": {
            "globalId_content_dict_list": str(globalId_content_dict_list), # 使用json字符串傳遞複雜結構
            "globalId_title_dict_list": str(globalId_title_dict_list)
        },
        "response_mode": "blocking",
        "user": user
    }
    if workflow_id:
        data["workflow_id"] = workflow_id
        
    return await _run_workflow_async(api_key, data)