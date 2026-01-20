import requests
import json
import os
import time
from datetime import datetime
import httpx
import re
from docxtpl import DocxTemplate, InlineImage, RichText  # 用於生成Word文檔
from docx.shared import Mm  # 用於設置Word文檔的尺寸
import glob  # 用於查找文件
import logging
from PIL import Image
# import random
import asyncio
# import aiohttp
# import aiofiles
from typing import Any, List, Dict, Optional
from urllib.parse import urlparse
from bs4 import BeautifulSoup
# import fitz  # PyMuPDF
# from pdf_image_extractor import PDFImageExtractor  # 導入PDFImageExtractor
# from curl_cffi import requests as cffi_requests

# 導入自定義模塊
from pdf_utils import process_pdf_with_extractor, convert_pdf_to_image, download_pdf
from image_utils import download_images_with_timestamp, validate_and_convert_image
from data_utils import getData, create_product_dict, transform_mydict_to_mydict_list_final, html_to_markdown, clean_old_files
from api_utils import (
    upload_file_pdf_pdf2content,upload_file_image_pdf2content, 
    run_workflow_pdf_and_image_pdf2content, run_workflow_pdf_pdf2content,
    run_workflow_foodsafety
)

from dotenv import load_dotenv

load_dotenv()
API_WORKFLOW_RUN_URL_PRO = os.getenv("API_WORKFLOW_RUN_URL_PRO")
API_KEY_PRO_V2 = os.getenv("API_KEY_PRO_V2")  # workflow : foodsafety
API_KEY_PRO_PDF2CONTENT = os.getenv("API_KEY_PRO_PDF2CONTENT")  # workflow : PDF2Content

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_json(data, userId):
    """
    創建JSON數據
    
    Args:
        data(dict) : 傳入的原始數據:
            {
                "globalIds":[
                    "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa",
                    "bmV3c0A2ODY2YzhiNWRjODY1ODM2NmI4MWIyMjFAMjAyNS0wNi0yNVQxNjowMDowMC4wMDBa" 
                ],
                "imagesByGlobalId":{
                    "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa":[
                        "https://www.mpi.govt.nz/assets/On-page-images/Food-Recalls/2025/June/Woori-Kimchi-brand-Minced-Raw-Garlic.jpg"
                    ],
                    "bmV3c0A2ODY2YzhiNWRjODY1ODM2NmI4MWIyMjFAMjAyNS0wNi0yNVQxNjowMDowMC4wMDBa":[

                    ],
                },
                "userId":"test"
            }

        userId (str): 用戶ID

    Returns:
        list: 最終的 myDictFinalList 列表
    """

    globalID_list = data["globalIds"]

    # [1] 獲取食品召回產品召回條目
    raw_data = getData(globalID_list)  

    # [2] 處理FSIS和FSA的PDF鏈接
    logger.info("\n首先處理媒體來源為FSIS和FSA的PDF鏈接 - PDF圖片提取 ....\n"+ "=" * 60)

    try:
        if raw_data is None:
            raw_data = []
        elif not isinstance(raw_data, list):
            raw_data = []
            
        patterns = [
            r'href="(https://www.fsis.usda.gov/sites/default/files/food_label_pdf/[^"]+\.pdf)"',
            r'href="(https://s3-eu-west-1.amazonaws.com/fsa-alerts-files/production/[^"]+\.pdf)"'
        ]
        
        pdf_dir = "data/pdf_files_from_fsis_fsa"
        images_dir = "data/images"
        os.makedirs(pdf_dir, exist_ok=True)
        os.makedirs(images_dir, exist_ok=True)
        
        async def process_item(item):
            try:
                if not isinstance(item, dict):
                    return
                    
                content = item.get('content', '')
                global_id = item.get('globalId', '')
                
                if not content or not global_id:
                    return
                
                # 遍歷查找PDF鏈接
                for pattern in patterns:
                    try:
                        matches = re.finditer(pattern, content)
                        for match in matches:
                            try:
                                pdf_link = match.group(1)
                                # 處理PDF文件
                                if await process_pdf_with_extractor(global_id, pdf_link, pdf_dir, images_dir):
                                    return  # 成功處理一個PDF後退出
                            except Exception as e:
                                continue  # 繼續處理下一個鏈接
                    except Exception as e:
                        continue
            except Exception as e:
                pass

        # 並發處理所有項目
        tasks = [process_item(item) for item in raw_data]
        await asyncio.gather(*tasks)
                
    except Exception as e:
        logger.error(f"處理媒體來源為FSIS和FSA的PDF鏈接任務時 - PDF圖片提取,並發執行出錯: {str(e)}")
    finally:
        logger.info("\n處理媒體來源為FSIS和FSA的PDF鏈接任務 - PDF圖片提取,完成！\n"+ "=" * 60)

    # 再次獲取原始數據,防止下载PDF文件后，raw_data 会发生变化
    raw_data = getData(data["globalIds"])
    # print("raw_data:::",raw_data)
    

    # [3] 數據構建 : For Dify
    # 以列表嵌套字典的方式存儲 globalId 和 content
    globalId_content_dict_list = []
    for item in raw_data:
        # 去除 html 標籤
        content_cleaned = html_to_markdown(item["content"])
        # 去除換行符、連續空格等
        content_cleaned = re.sub(pattern=r'\s+', repl=' ', string=content_cleaned).strip()
        globalId_content_dict_list.append({
            "globalId": item.get("globalId", ""),
            "content": content_cleaned,
            # 新增一個欄位 : 對於在python腳本中本身就涉及了對某些來源的distribution和recycling_reason就有特殊處理的
            # 那麼在Dify應用裡面就不要對其進行LLM判斷了
            "from": item.get("from", "") 
        })

    # print("globalId_content_dict_list:::",globalId_content_dict_list)
    
    # 以列表的方式單獨地存儲content
    content_list = []
    for item in raw_data:
        # 去除 html 標籤
        content_cleaned = html_to_markdown(item["content"])
        # 去除換行符、連續空格等
        content_cleaned = re.sub(pattern=r'\s+', repl=' ', string=content_cleaned).strip()
        content_list.append(content_cleaned)

    # 以列表嵌套字典的方式存儲 globalId 和 title
    globalId_title_dict_list = []
    for item in raw_data:
        globalId_title_dict_list.append({
            "globalId": item.get("globalId", ""),
            "title": item.get("title", "")
        })

    # print("globalId_title_dict_list:::",globalId_title_dict_list)
    # 以列表的方式單獨地存儲title
    title_list = []
    for item in raw_data:
        title_list.append(item["title"])

    
    # 以列表嵌套字典的方式存儲 globalId 和 from
    globalId_from_dict_list = []
    for item in raw_data:
        globalId_from_dict_list.append({
            "globalId": item.get("globalId", ""),
            "from": item.get("from", "")
        })

    # 以列表的方式單獨地存儲from
    from_list = []
    for item in raw_data:
        from_list.append(item["from"])

    # 以列表嵌套字典的方式存儲 globalId 和 url
    globalId_url_dict_list = []
    for item in raw_data:
        globalId_url_dict_list.append({
            "globalId": item.get("globalId", ""),
            "url": item.get("url", "")
        })

    # print("globalId_url_dict_list:::",globalId_url_dict_list)

    # 以列表的方式單獨地存儲url
    url_list = []
    for item in raw_data:
        url_list.append(item["url"])


    # [4] 根據 raw_data 對不同的來源進行數據處理
    # RASFF : 提取所有來源為 RASFF 的 content,通過 content 拿到對應的 title ,並存儲到 rasff_title_dict_list 列表中
    rasff_title_dict_list = []
    for item in raw_data:
        if item.get('from') == 'RASFF':
            try:
                original_content = item['content']
                rasff_pattern = r'<h3>\s*Subject\s*</h3>\s*<p>\s*<span>\s*(.*?)\s*</span>'
                match = re.search(rasff_pattern, original_content)
                if match:
                    rasff_title_dict_list.append({item.get("globalId"):match.group(1).strip()})
            except:
                rasff_title_dict_list.append({item.get("globalId"):"--"})

    # FDA : 提取所有來源為 FDA 的 content，去除html標籤後,通過去除html標籤的 content 拿到對應的 recycling_reason,並存儲到 fda_recycling_reason_list 列表中
    fda_recycling_reason_dict_list = []
    fda_pattern = r'Recall Reason Description\s*(.*?)\s*Company Name:'
    for item in raw_data:
        if item.get('from') == 'The US Food and Drug Administration (FDA)':  
            try:
                content_markdown = html_to_markdown(item['content'])
                content_cleaned = re.sub(r'\s+', ' ', content_markdown).strip()
                match = re.search(fda_pattern, content_cleaned)
                if match:
                    fda_recycling_reason_dict_list.append({item.get("globalId"):match.group(1).strip()})
            except Exception as e:
                fda_recycling_reason_dict_list.append({item.get("globalId"):"--"})

    # 處理 Government of Canada 數據 : distribution
    canada_distribution_dict_list = []
    for item in raw_data:
        if item.get("from") == "Government of Canada":
            try:
                original_content = html_to_markdown(item["content"])
                pattern = r'Distribution\s*(.*?)\s*Affected'
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 1. 原始提取的結果
                    raw_result = match.group(1)
                    # 2. 按換行符分割，清理每一行，並過濾掉空行
                    lines = raw_result.split('\n')
                    cleaned_lines = [line.strip() for line in lines if line.strip()]
                    # 3. 使用換行符將清理後的行重新組合成一個字串
                    canada_distribution = '\n'.join(cleaned_lines)
                    canada_distribution_dict_list.append({item.get("globalId"):canada_distribution})
                else:
                    canada_distribution_dict_list.append({item.get("globalId"):"--"})
            except:
                canada_distribution_dict_list.append({item.get("globalId"):"--"})

    # 處理 The Food Standards Australia New Zealand (FSANZ) 數據 : recycling_reason
    fsanz_recycling_reason_dict_list = []
    for item in raw_data:
        if item.get("from") == "The Food Standards Australia New Zealand (FSANZ)":
            try:
                original_content = html_to_markdown(item["content"])
                # pattern = r'\*\*Problem:\*\*\s*(.*?)\s*\*\*Food safety hazard:\*\*'
                pattern = r'(?:\*\*)?Problem:\s*(.*?)\s*(?:\*\*)?Food safety hazard:'  # ** 可選
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 提取匹配的結果
                    fsanz_recycling_reason = match.group(1).replace("*","").strip()
                    fsanz_recycling_reason_dict_list.append({item.get("globalId"):fsanz_recycling_reason})
                else:
                    fsanz_recycling_reason_dict_list.append({item.get("globalId"):"--"})
            except:
                fsanz_recycling_reason_dict_list.append({item.get("globalId"):"--"})

    # 處理 The Food Standards Australia New Zealand (FSANZ) 數據 : distribution
    fsanz_distribution_dict_list = []
    for item in raw_data:
        if item.get("from") == "The Food Standards Australia New Zealand (FSANZ)":
            try:
                original_content = html_to_markdown(item["content"])
                # 步驟 1: 提取關鍵字 "Date Marking" 之前的所有內容
                # 使用正則表達式分割，忽略大小寫，只分割一次，取第一部分
                text_before_date_marking = re.split(r'Date Marking', original_content, maxsplit=1, flags=re.IGNORECASE)[0]
                
                # 步驟 2: 在此基礎上，提取所有包含 "available for sale" 的句子
                keyword_to_find = "available for sale"
                found_sentences = []
                
                # 使用句號 '.' 切分句子
                potential_sentences = text_before_date_marking.split('.')
                
                # 遍歷所有切分出的潛在句子
                for sentence in potential_sentences:
                    # 檢查句子（轉換為小寫）是否包含關鍵字
                    if keyword_to_find in sentence.lower():
                        # 清理句子前後的空白，並確保不是空字符串
                        cleaned_sentence = sentence.strip()
                        if cleaned_sentence:
                            # 將清理後的句子加上句號，添加到結果列表
                            found_sentences.append(cleaned_sentence + ".")
                
                # 步驟 3: 將所有找到的句子合併成一個字符串，用空格隔開
                distribution_details = ' '.join(found_sentences)
                
                # 步驟 4 :  如果成功提取到內容，繼續對提取到的內容進行數據處理
                if distribution_details:
                    # 匹配 "The product(s) have been" 或 "The products have been"（忽略大小寫)
                    pattern = re.compile(r'The product\(s\) have been|The products have been', re.IGNORECASE)
                    match = pattern.search(distribution_details)
                    if match:
                        # 獲取匹配開始之前的所有內容
                        prefix = distribution_details[:match.start()]
                        # 如果前面有實質性內容（而不僅僅是空格）
                        if prefix.strip():
                            # 則僅提取從匹配項開始的內容
                            distribution_details = distribution_details[match.start():]

                    # 無論是否經過二次處理，都將最終的 distribution_details 添加到列表中
                    # 如果沒有找到匹配項，或匹配項在開頭，則添加原始的 distribution_details
                    fsanz_distribution_dict_list.append({item.get("globalId"): distribution_details})
                      
                else:
                    fsanz_distribution_dict_list.append({item.get("globalId"): "--"})
            except:
                fsanz_distribution_dict_list.append({item.get("globalId"): "--"})
        
    # 處理 Ministry for Primary Industries (MPI) 數據 : 副標題 recycling_reason
    mpi_recycling_reason_dict_list = []
    for item in raw_data:
        if item.get("from") == "Ministry for Primary Industries (MPI)":
            try:
                # 副標題在 <h5> </h5> 標記中,這裡不需要使用 html_to_markdown()函數,直接根據 <h5> </h5> 標記 提取副標題內容
                original_content = item["content"]
                pattern = r"<h5><p>(.*?)</p></h5>"
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 1. 原始提取的結果
                    mpi_recycling_reason = match.group(1).strip()
                    # 2. 將當前對應的 globalId 作為鍵, 匹配的結果 作為值, 存儲到字典中
                    mpi_recycling_reason_dict_list.append({item.get("globalId"):mpi_recycling_reason})
                else:
                    mpi_recycling_reason_dict_list.append({item.get("globalId"):"--"})
            except:
                mpi_recycling_reason_dict_list.append({item.get("globalId"):"--"})

    # 處理 Ministry for Primary Industries (MPI) 數據 : distribution
    mpi_distribution_dict_list = []
    # 提取Distribution到Notes、Distribution到Point of sale notice for retailers、
    # Distribution到Point of sale notices for retailers、Distribution到Consumer advice中間的內容
    pattern = re.compile(
    # 匹配 "Distribution" 後，非貪婪地匹配任何字符，直到遇到它的關閉標籤 </td>
    r"Distribution.*?</td>"
    # 匹配兩個儲存格之間的空白
    r"\s*"
    # 匹配內容儲存格的開始標籤，允許帶有屬性
    r"<td[^>]*>"
    # 捕獲我們需要的核心 HTML 內容
    r"\s*(.*?)\s*"
    # 匹配內容儲存格的關閉標籤
    r"</td>"
    # 非貪婪地匹配直到結束關鍵字
    r".*?"
    # 匹配結束關鍵字
    r"(?:Notes|Point of sale notice for retailers|Consumer advice|Point of sale notices for retailers|Point of sale notice)",
    re.DOTALL | re.IGNORECASE
)
    for item in raw_data:
        if item.get("from") == "Ministry for Primary Industries (MPI)":
            try:
                original_content = item.get("content", "")
                match = pattern.search(original_content)

                if match:
                    html_content = match.group(1)
                    soup = BeautifulSoup(html_content, 'lxml')

                    # 遍歷每一個<a>標籤並在原地替換它
                    for a_tag in soup.find_all('a', href=True):
                        text = a_tag.get_text(strip=True)
                        href = a_tag.get('href')
                        # 判斷和替換的邏輯現在在 for 循環內部
                        if text and href:
                            special_link_string = f"§HYPERLINK§{text}§{href}§"
                            # 替換當前正在處理的 a_tag
                            a_tag.replace_with(special_link_string)
                    
                    # 如果沒有超鏈接，soup 對象保持原樣，這一步仍然會正確提取純文本
                    lines = [line.strip() for line in soup.get_text(separator='\n').split('\n') if line.strip()]
                    result_text = '\n'.join(lines)
                    mpi_distribution_dict_list.append({item.get("globalId"): result_text})
                
            except Exception as e:
                logger.error(f"處理 MPI distribution 時出錯，ID: {item.get('globalId')}: {e}")
                mpi_distribution_dict_list.append({item.get("globalId"): "--"})

    # 處理 Food Standards Agency 數據 : distribution 
    fsa_distribution_dict_list = []
    for item in raw_data:
        if item.get("from") == "Food Standards Agency":
            try:
                # 副標題在 <h5> </h5> 標記中,這裡不需要使用 html_to_markdown()函數,直接根據 <h5> </h5> 標記 提取副標題內容
                original_content = item["content"]
                pattern = r"<h5>(.*?)</h5>"
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 1. 原始提取的結果
                    raw_result = match.group(1).strip()
                    # 2. 將當前對應的 globalId 作為鍵, 匹配的結果 作為值, 存儲到字典中
                    fsa_distribution_dict_list.append({item.get("globalId"):raw_result})
                else:
                    fsa_distribution_dict_list.append({item.get("globalId"):"--"})
            except:
                fsa_distribution_dict_list.append({item.get("globalId"):"--"})

    # 處理 Rappel Conso 數據 : distribution
    rappel_conso_distribution_dict_list = []
    for item in raw_data:
        if item.get("from") == "Rappel Conso":
            try:
                original_content = html_to_markdown(item["content"])
                pattern = r"Zone géographique de vente(.*?)Distributeurs(.*?)Informations pratiques concernant le rappel"
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 1. 提取匹配的兩個結果
                    zone = match.group(1).strip()
                    distributeurs = match.group(2).strip()
                    # 2. 將這兩個記過進行合並
                    rappel_conso_distribution = f"{zone} : {distributeurs}"
                    # 3. 將當前對應的 globalId 作為鍵, 匹配的結果 作為值, 存儲到字典中
                    rappel_conso_distribution_dict_list.append({item.get("globalId"):rappel_conso_distribution})
                else:
                    rappel_conso_distribution_dict_list.append({item.get("globalId"):"--"})
            except:
                rappel_conso_distribution_dict_list.append({item.get("globalId"):"--"})

    # 處理 Rappel Conso 數據 : recycling_reason
    rappel_conso_recycling_reason_dict_list = []
    for item in raw_data:
        if item.get("from") == "Rappel Conso":
            try:
                original_content = html_to_markdown(item["content"])
                pattern = r"Motif du rappel(.*?)Risques encourus par le consommateur"
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 1. 提取匹配的結果
                    rappel_conso_recycling_reason = match.group(1).strip()
                    # 2. 將當前對應的 globalId 作為鍵, 匹配的結果 作為值, 存儲到字典中
                    rappel_conso_recycling_reason_dict_list.append({item.get("globalId"):rappel_conso_recycling_reason})
                else:
                    rappel_conso_recycling_reason_dict_list.append({item.get("globalId"):"--"})
            except:
                rappel_conso_recycling_reason_dict_list.append({item.get("globalId"):"--"})

    # 處理 NSW Food Authority 數據 : recycling_reason
    nsw_recycling_reason_dict_list = []
    for item in raw_data:
        if item.get("from") == "NSW Food Authority":
            try:
                original_content = html_to_markdown(item["content"])
                # pattern = r'\*\*Problem:\*\*\s*(.*?)\s*\*\*Food safety hazard:\*\*'
                pattern = r'(?:\*\*)?Problem:\s*(.*?)\s*(?:\*\*)?Food safety hazard:'  # ** 可選
                match = re.search(pattern, original_content, re.DOTALL)
                if match:
                    # 提取匹配的結果
                    nsw_recycling_reason = match.group(1).replace("*", "").strip()
                    nsw_recycling_reason_dict_list.append({item.get("globalId"): nsw_recycling_reason}) 
                else:
                    nsw_recycling_reason_dict_list.append({item.get("globalId"): "--"})
            except:
                nsw_recycling_reason_dict_list.append({item.get("globalId"): "--"})

    # 處理 NSW Food Authority 數據 : distribution
    nsw_distribution_dict_list = []
    for item in raw_data:
        if item.get("from") == "NSW Food Authority":
            try:
                original_content = html_to_markdown(item["content"])
                # 步驟 1: 提取關鍵字 "Date Marking" 之前的所有內容
                # 使用正則表達式分割，忽略大小寫，只分割一次，取第一部分
                text_before_date_marking = re.split(r'Date Marking', original_content, maxsplit=1, flags=re.IGNORECASE)[0]

                # 步驟 2: 在此基礎上，提取所有包含 "available for sale" 的句子
                keyword_to_find = "available for sale"
                found_sentences = []

                # 使用句號 '.' 切分句子
                potential_sentences = text_before_date_marking.split('.')

                # 遍歷所有切分出的潛在句子
                for sentence in potential_sentences:
                    # 檢查句子（轉換為小寫）是否包含關鍵字
                    if keyword_to_find in sentence.lower():
                        # 清理句子前後的空白，並確保不是空字符串
                        cleaned_sentence = sentence.strip()
                        if cleaned_sentence:
                            # 將清理後的句子加上句號，添加到結果列表
                            found_sentences.append(cleaned_sentence + ".")

                # 步驟 3: 將所有找到的句子合併成一個字符串，用空格隔開
                distribution_details = ' '.join(found_sentences)

                # 步驟 4 :  如果成功提取到內容，繼續對提取到的內容進行數據處理
                if distribution_details:
                    # 匹配 "The product(s) have been" 或 "The products have been"（忽略大小寫)
                    pattern = re.compile(r'The product\(s\) have been|The products have been', re.IGNORECASE)
                    match = pattern.search(distribution_details)
                    if match:
                        # 獲取匹配開始之前的所有內容
                        prefix = distribution_details[:match.start()]
                        # 如果前面有實質性內容（而不僅僅是空格）
                        if prefix.strip():
                            # 則僅提取從匹配項開始的內容
                            distribution_details = distribution_details[match.start():]

                    # 無論是否經過二次處理，都將最終的 distribution_details 添加到列表中
                    # 如果沒有找到匹配項，或匹配項在開頭，則添加原始的 distribution_details
                    nsw_distribution_dict_list.append({item.get("globalId"): distribution_details})
                      
                else:
                    nsw_distribution_dict_list.append({item.get("globalId"): "--"})
            except:
                nsw_distribution_dict_list.append({item.get("globalId"): "--"})

    # 處理 消費者廳 數據 : distribution 和 recycling_reason
    japan_distribution_dict_list = []
    japan_recycling_reason_dict_list = []
    # 定義主要標記，優先搜索
    jp_distribution_markers = [
        "販売地域、販売先：",
        "販売チャネル：",
        "販売店舗：",
        "販売場所：",
        "販売地域：",
        "販売先 ：",
        "販売店 ：",
        "場所：",
        "を販売している地区や地域：",
    ]
    # 定義備用標記，僅在未找到主要標記時搜索
    jp_distribution_markers_fallback = [
        "その他："
    ]
    for item in raw_data:
        if item.get("from") == "消費者廳":
            try:
                original_content = html_to_markdown(item["content"])
                lines = original_content.split('\n')
                
                # --- Distribution Extraction ---
                found_distributions = []
                
                # 第一輪搜索：主要標記
                for i, line in enumerate(lines):
                    line_stripped = line.strip()
                    for marker in jp_distribution_markers:
                        if '：' in marker:
                            # 提取標記文本（去除冒號和前後空格）
                            marker_text = marker.split('：')[0].strip()
                            # 構建正則表達式：匹配標記文本 + 任意空白字符（含全角空格） + 冒號
                            pattern = re.compile(re.escape(marker_text) + r'\s*：')
                            
                            match = pattern.search(line_stripped)
                            if match:
                                # 提取冒號之後的內容
                                content = line_stripped[match.end():].strip()
                                if content:
                                    found_distributions.append(content)
                                
                                # 檢查後續行是否有縮進（全角或半角空格），如果有則視為連續內容
                                j = i + 1
                                while j < len(lines):
                                    next_line = lines[j]
                                    if next_line.startswith('\u3000') or next_line.startswith(' '):
                                        extended_content = next_line.strip()
                                        if extended_content:
                                            found_distributions.append(extended_content)
                                        j += 1
                                    else:
                                        break
                                break 
                
                # 如果第一輪搜索沒有找到任何結果，則進行第二輪搜索：備用標記
                if not found_distributions:
                    for i, line in enumerate(lines):
                        line_stripped = line.strip()
                        for marker in jp_distribution_markers_fallback:
                            if '：' in marker:
                                marker_text = marker.split('：')[0].strip()
                                pattern = re.compile(re.escape(marker_text) + r'\s*：')
                                match = pattern.search(line_stripped)
                                if match:
                                    content = line_stripped[match.end():].strip()
                                    if content:
                                        found_distributions.append(content)
                                    
                                    # 檢查後續行是否有縮進（全角或半角空格），如果有則視為連續內容
                                    j = i + 1
                                    while j < len(lines):
                                        next_line = lines[j]
                                        if next_line.startswith('\u3000') or next_line.startswith(' '):
                                            extended_content = next_line.strip()
                                            if extended_content:
                                                found_distributions.append(extended_content)
                                            j += 1
                                        else:
                                            break
                                    break

                if found_distributions:
                    # 去重，保持順序
                    seen = set()
                    unique_distributions = []
                    for dist in found_distributions:
                        if dist not in seen:
                            seen.add(dist)
                            unique_distributions.append(dist)
                    
                    found_distributions = unique_distributions

                    # 檢查是否包含 "全國" 或 "全国"
                    has_national = False
                    for i, dist in enumerate(found_distributions):
                        if "全國" in dist or "全国" in dist:
                            # 找到了，將其移除並保存
                            found_distributions.pop(i)
                            has_national = True
                            break # 假設只有一個包含全國的項，或者只處理第一個
                    
                    # 如果包含全國，將 "全国" 添加到列表的最前面
                    if has_national:
                         found_distributions.insert(0, "全国")

                    distribution_details = "\n".join(found_distributions)
                    
                    # 處理 "参照情報をご確認ください。" 的特殊情況
                    if "参照情報をご確認ください。" in distribution_details:
                        # 嘗試從原始 content 中提取鏈接
                        try:
                            soup = BeautifulSoup(item["content"], 'html.parser')
                            # 尋找文本為 "参照情報" 的 a 標籤
                            link_tag = soup.find('a', string='参照情報')
                            if link_tag and link_tag.get('href'):
                                extracted_link = link_tag['href']
                                # 構造 RichText 可識別的超鏈接格式: §HYPERLINK§顯示文本§URL§剩餘文本
                                distribution_details = distribution_details.replace("参照情報をご確認ください。", f"§HYPERLINK§参照情報§{extracted_link}§")
                        except Exception as e:
                            logger.error(f"提取参照情報鏈接時出錯: {str(e)} - ID: {item.get('globalId')}")

                    japan_distribution_dict_list.append({item.get("globalId"): distribution_details})
                else:
                    japan_distribution_dict_list.append({item.get("globalId"): "--"})

                # --- Recycling Reason Extraction ---
                found_reasons = []
                reason_marker = "回収理由の詳細："
                start_collecting = False
                
                for line in lines:
                    line_stripped = line.strip()
                    if reason_marker in line_stripped:
                        start_collecting = True
                        # Check if content is on the same line after marker
                        parts = line_stripped.split(reason_marker, 1)
                        if len(parts) > 1 and parts[1].strip():
                            found_reasons.append(parts[1].strip())
                        continue
                    
                    if start_collecting:
                        if not line_stripped: # Stop at empty line
                            break
                        found_reasons.append(line_stripped)
                
                if found_reasons:
                    reason_details = "\n".join(found_reasons)
                    japan_recycling_reason_dict_list.append({item.get("globalId"): reason_details})
                else:
                    japan_recycling_reason_dict_list.append({item.get("globalId"): "--"})

            except Exception as e:
                logger.error(f"Error processing item {item.get('globalId')}: {e}")
                japan_distribution_dict_list.append({item.get("globalId"): "--"})
                japan_recycling_reason_dict_list.append({item.get("globalId"): "--"})
    
    # print("japan_distribution_dict_list:", japan_distribution_dict_list)
    # print("japan_recycling_reason_dict_list:", japan_recycling_reason_dict_list)
   

    # [5] 下載相關來源的PDF文件
    # [5.1] 下載來源為 CDPH 的 PDF 
    pattern_cdph = r'^(<a )?href="https://www\.[^\s"]+\.pdf"'
    cdph_pdf_file_downloaded_path_list = []  # 用於存儲 CDPH 文章的 PDF 文件下載路徑列表
    cdph_distribution_additional_list = []  # 用於存儲經過 process_pdf()函數的 CDPH 文章的 Distribution 列表(零售商信息)
    cdph_pdf_image_path_list = []  # 用於存儲 CDPH 文章的 PDF 圖片路徑列表

    async def process_cdph_item(item):
        global_id = item['globalId']
        content = item['content']

        if re.match(pattern_cdph, content):
            pdf_url_match = re.search(r'href="([^"]+)"', content)
            target_cdph = "/CEH/DFDCS/CDPH"

            if pdf_url_match and target_cdph in (pdf_url := str(pdf_url_match.group(1))):
                data_dir = os.path.join("data", "pdf_files")
                output_filename = os.path.join(data_dir, f"cdph_{global_id}.pdf")
                pdf_file_downloaded_path = None

                if os.path.exists(output_filename):
                    logger.info(f"CDPH PDF 文件已存在，跳過下載: {output_filename}")
                    pdf_file_downloaded_path = output_filename
                else:
                    max_retries = 3
                    retry_count = 0
                    while retry_count < max_retries:
                        downloaded_path = await download_pdf(pdf_url, output_filename)
                        if downloaded_path:
                            pdf_file_downloaded_path = downloaded_path
                            break
                        else:
                            retry_count += 1
                            logger.warning(f"下載CDPH PDF失敗，正在重試({retry_count}/{max_retries})...")
                            await asyncio.sleep(2)

                if pdf_file_downloaded_path:
                    cdph_pdf_file_downloaded_path_list.append(pdf_file_downloaded_path)

                    output_dir = os.path.join("data", "pdf_images_ocr")
                    output_format = 'png'
                    target_image_path = os.path.join(output_dir, f"{global_id}.{output_format}")
                    final_image_path = None

                    if os.path.exists(target_image_path):
                        logger.info(f"CDPH 圖片已存在，跳過轉換: {target_image_path}")
                        final_image_path = target_image_path
                    else:
                        final_image_path = convert_pdf_to_image(pdf_file_downloaded_path, output_dir, output_format, dpi=200)

                    if final_image_path:
                        cdph_pdf_image_path_list.append(final_image_path)

    # [5.2] 下載來源為 HK 的 PDF
    hk_pdf_file_downloaded_path_list = []
    pattern_hk = r'^https://www\.cfs\.gov\.hk/.*\.pdf$'

    async def process_hk_item(item):
        global_id = item['globalId']
        pdf_url = item['url']

        if re.match(pattern_hk, pdf_url):
            data_dir = os.path.join("data", "pdf_files")
            output_filename = os.path.join(data_dir, f"hk_{global_id}.pdf")
            pdf_file_downloaded_path = None

            if os.path.exists(output_filename):
                logger.info(f"HK PDF 文件已存在，跳過下載: {output_filename}")
                pdf_file_downloaded_path = output_filename
            else:
                max_retries = 3
                retry_count = 0
                while retry_count < max_retries:
                    downloaded_path = await download_pdf(pdf_url, output_filename)
                    if downloaded_path:
                        pdf_file_downloaded_path = downloaded_path
                        break
                    else:
                        retry_count += 1
                        await asyncio.sleep(2)

            if pdf_file_downloaded_path:
                hk_pdf_file_downloaded_path_list.append(pdf_file_downloaded_path)

    try:
        # 創建所有CDPH項目的任務
        cdph_tasks = [process_cdph_item(item) for item in globalId_content_dict_list]
        # 創建所有HK項目的任務
        hk_tasks = [process_hk_item(item) for item in globalId_url_dict_list]
        
        # 並發執行所有任務
        await asyncio.gather(*cdph_tasks, *hk_tasks)

        if not cdph_pdf_file_downloaded_path_list:
            logger.warning("沒有找到任何一個相關的 CDPH PDF 文件鏈接,無需下載")
        if not hk_pdf_file_downloaded_path_list:
            logger.warning("沒有找到任何一個相關的 HK PDF 文件鏈接,無需下載")

    except Exception as e:
        logger.error(f"處理PDF文件時發生錯誤: {str(e)}")


    # [6] Dify 工作流的執行
    # [6.1] 相關變量初始化
    api_key_pro_v2 = API_KEY_PRO_V2  # workflow : foodsafety
    api_key_pro_pdf2content = API_KEY_PRO_PDF2CONTENT  # workflow : PDF2Content
    user = userId
    workflow_id = None  # 如果需要指定工作流ID，請在這裡設置

    # 定義批處理大小
    WORKFLOW_CHUNK_SIZE = 9

    # 初始化用於匯總所有批次結果的列表
    cdph_title_list = []
    cdph_distribution_list = []
    hk_distribution_list = []
    cdph_title_dict_list = []
    cdph_distribution_dict_list = []
    hk_distribution_dict_list = []
    globalId_distribution_dict_list = []
    globalId_recyclingReason_dict_list = []
    globalId_isOrNot_dict_list = []

    
    # 匯總所有的 pdf_file_downloaded_path_list : 目前只有 CDPH 和 HK
    if cdph_pdf_file_downloaded_path_list and hk_pdf_file_downloaded_path_list:
        gather_pdf_file_downloaded_path_list = cdph_pdf_file_downloaded_path_list + hk_pdf_file_downloaded_path_list    
    elif cdph_pdf_file_downloaded_path_list:
        gather_pdf_file_downloaded_path_list = cdph_pdf_file_downloaded_path_list
    elif hk_pdf_file_downloaded_path_list:
        gather_pdf_file_downloaded_path_list = hk_pdf_file_downloaded_path_list
    else:
        logger.warning("沒有找到任何一個相關的 PDF 文件鏈接,無需下載")

    # 匯總所有的 pdf_image_path_list : 目前只有 CDPH
    if cdph_pdf_image_path_list:
        gather_pdf_image_path_list = cdph_pdf_image_path_list
    else:
        gather_pdf_image_path_list = []

    # [6.2] 並發上傳所有文件 
    # 將關聯的 PDF 和 Image 組合成一個處理單元，以便統一分批
    file_processing_list = []
    # 添加 CDPH 文件 (有對應的圖片)
    for pdf_path, img_path in zip(cdph_pdf_file_downloaded_path_list, cdph_pdf_image_path_list):
        file_processing_list.append({'pdf': pdf_path, 'image': img_path})
    # 添加 HK 文件 (沒有圖片)
    for pdf_path in hk_pdf_file_downloaded_path_list:
        file_processing_list.append({'pdf': pdf_path, 'image': None})

    if not file_processing_list:
        logger.info("沒有找到需要通過 PDF2Content 工作流處理的文件。")
    else:
        # --- 6.3 對統一隊列進行分批並處理 ---
        processing_chunks = [file_processing_list[i:i + WORKFLOW_CHUNK_SIZE] for i in range(0, len(file_processing_list), WORKFLOW_CHUNK_SIZE)]
        logger.info(f"文件已準備就緒，將分成 {len(processing_chunks)} 個批次執行上傳和工作流。")

        for i, chunk in enumerate(processing_chunks):
            logger.info(f"--- 正在處理批次 {i + 1}/{len(processing_chunks)} ---")
            
            # 提取當前批次需要上傳的文件路徑
            pdf_paths_in_chunk = [item['pdf'] for item in chunk if item['pdf']]
            image_paths_in_chunk = [item['image'] for item in chunk if item['image']]

            try:
                # 步驟 A: 並發上傳當前批次的文件
                logger.info(f"批次 {i + 1}: 上傳 {len(pdf_paths_in_chunk)} 個 PDF 和 {len(image_paths_in_chunk)} 個圖片...")
                upload_tasks = [
                    upload_file_pdf_pdf2content(pdf_paths_in_chunk, api_key_pro_pdf2content, user),
                    upload_file_image_pdf2content(image_paths_in_chunk, api_key_pro_pdf2content, user)
                ]
                pdf_ids_in_chunk, image_ids_in_chunk = await asyncio.gather(*upload_tasks)
                
                if not pdf_ids_in_chunk:
                    logger.warning(f"批次 {i + 1}: 未能成功上傳任何 PDF 文件，跳過此批次的工作流執行。")
                    continue
                
                logger.info(f"批次 {i + 1}: 上傳完成。")

                # 步驟 B: 根據當前批次是否有圖片，執行對應的工作流
                if image_ids_in_chunk:
                    # 情況一: 當前批次包含圖片
                    result = await run_workflow_pdf_and_image_pdf2content(
                        pdf_file_ids=pdf_ids_in_chunk,
                        image_file_ids=image_ids_in_chunk, # 只傳入當前批次的圖片ID
                        api_key=api_key_pro_pdf2content, user=user,
                        workflow_id=workflow_id
                    )
                    outputs = result.get('data', {}).get('outputs', {})
                    # extend : 將每個批次中的 cdph_title_list 添加到 cdph_title_list 列表中
                    cdph_title_list.extend(outputs.get('cdph_title_list', []))  
                    cdph_distribution_list.extend(outputs.get('cdph_distribution_list', []))
                    hk_distribution_list.extend(outputs.get('hk_distribution_list', []))
                    cdph_title_dict_list.extend(outputs.get('cdph_title_dict_list', []))
                    cdph_distribution_dict_list.extend(outputs.get('cdph_distribution_dict_list', []))
                    hk_distribution_dict_list.extend(outputs.get('hk_distribution_dict_list', []))
                    
                else:
                    # 情況二: 當前批次只包含PDF
                    result = await run_workflow_pdf_pdf2content(
                        pdf_file_ids=pdf_ids_in_chunk,
                        api_key=api_key_pro_pdf2content, user=user,
                        workflow_id=workflow_id
                    )
                    outputs = result.get('data', {}).get('outputs', {})
                    hk_distribution_list.extend(outputs.get('hk_distribution_list', []))
                    hk_distribution_dict_list.extend(outputs.get('hk_distribution_dict_list', []))
                
                logger.info(f"PDF2Content 工作流批次 {i + 1} 執行完成。")

            except Exception as e:
                logger.error(f"處理批次 {i + 1} 時發生錯誤: {e}", exc_info=True)
            
            await asyncio.sleep(1) # 批次間停頓

    # --- 6.4 執行 foodsafety 工作流 ---
    logger.info("開始執行 foodsafety 工作流...")
    try:
        result_foodsafety = await run_workflow_foodsafety(
            globalId_content_dict_list=globalId_content_dict_list,
            globalId_title_dict_list=globalId_title_dict_list,
            api_key=api_key_pro_v2, user=user, workflow_id=workflow_id
        )
        outputs = result_foodsafety.get('data', {}).get('outputs', {})
        globalId_distribution_dict_list = outputs.get('id_distribution_dict_list', [])
        globalId_recyclingReason_dict_list = outputs.get('id_recyclingReason_dict_list', [])
        globalId_isOrNot_dict_list = outputs.get('id_isOrNot_dict_list', [])
        logger.info("foodsafety 工作流執行完成。")
    except Exception as e:
        logger.error(f"執行 foodsafety 工作流時發生錯誤: {e}", exc_info=True)
        

    # [7] Dify 工作流執行後的數據處理
    # [7.1] 處理 distribution : word報告服務的美化 : 將distribution的內容進行美化,例如將地點之間的分隔符 '", "' 替換為換行符 '\n'
    # 1. 創建 global_id 到 distribution 的查找字典
    distribution_lookup = {item.get('global_id') or item.get('globalId'): item['distribution'] for item in globalId_distribution_dict_list}
    
    # 2. 根據 data["globalIds"] 的順序構建 distribution_list
    # 如果查找失敗，默認值為 "--"
    distribution_list = []
    for global_id in data["globalIds"]:
        dist_value = distribution_lookup.get(global_id, "--")
        # 額外的清理逻辑 (此前在循环中处理的)
        if dist_value != "--":
             # 1. 將地點之間的分隔符 '", "' 替換為換行符 '\n'
            processed_string = dist_value.replace('", "', '\n')
            # 2. 清理字符串兩端可能存在的空格和雙引號
            cleaned_string = processed_string.strip().strip('"')
            distribution_list.append(cleaned_string)
        else:
            distribution_list.append("--")

    # [7.2] 處理 recycling_reason 
    # 1. 創建 global_id 到 recycling_reason 的查找字典
    recycling_reason_lookup = {item.get('global_id') or item.get('globalId'): item['recycling_reason'] for item in globalId_recyclingReason_dict_list}

    # 2. 根據 data["globalIds"] 的順序構建 recycling_reason_list
    recycling_reason_list = []
    for global_id in data["globalIds"]:
        reason_value = recycling_reason_lookup.get(global_id, "--")
        if reason_value != "--":
            processed_item = reason_value.replace('", "', ',').strip()
            recycling_reason_list.append(processed_item)
        else:
            recycling_reason_list.append("--")

    # 創建自定義的字典格式
    myDict = create_product_dict(data, raw_data)
    # print("myDict:::",myDict)

    # 圖片url的下載
    await download_images_with_timestamp(
        myDict=myDict,
        images_dir="data/images",
        download_delay=3,
    )

    # 調用函數 - 根據自定義的字典轉成最終的 myDictFinal_list
    myDictFinalList = transform_mydict_to_mydict_list_final(
        myDict=myDict,
        distribution_list=distribution_list,
        recycling_reason_list=recycling_reason_list,
        verbose=True
    )

    # print("myDictFinalList_處理前:::",myDictFinalList)
    
    # [7.3] 根據 Dify 執行後的結果對不同的來源進行數據處理
    # [7.3.1]處理 CDPH 數據 和 HK 數據
    # 1. 創建查找字典
    def create_lookup_dict(dict_list):
        lookup_dict = {}
        for d in dict_list:
            lookup_dict.update(d)
        return lookup_dict

    # 2.1 創建 CDPH 數據的查找字典
    cdph_title_lookup = create_lookup_dict(cdph_title_dict_list)
    cdph_distribution_lookup = create_lookup_dict(cdph_distribution_dict_list)
    # 2.2 創建 HK 數據的查找字典
    hk_distribution_lookup = create_lookup_dict(hk_distribution_dict_list)

    # 3.1 處理 CDPH 數據
    # 判斷查找字典是否為空，如果不為空則進行處理
    if cdph_title_lookup or cdph_distribution_lookup:
        for item in myDictFinalList:
            if item.get("source") == "US CDPH":
                global_id = item.get("global_id")
                if not global_id:
                    continue  # 如果沒有 global_id，跳過此項

                # 使用 global_id 查找並更新 title
                new_title = cdph_title_lookup.get(global_id)
                if new_title:
                    item["title"] = new_title

                # 使用 global_id 查找並更新 distribution
                original_distribution_text = cdph_distribution_lookup.get(global_id)
                if original_distribution_text:
                    # 數據處理 : 清理換行符和多個空格的情況
                    cleaned_text = original_distribution_text.replace('\n', ' ')
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                    item["distribution"] = cleaned_text  # 先賦予清理後的值

                    # 如果包含 "Retail"，則嘗試生成並附加零售商列表URL
                    if "Retail" in cleaned_text:
                        try:
                            temp_url = re.sub(r'/[A-Za-z]+20\d{2}/', '/ProductandRetailDistributionLists/', item["url"])
                            retailer_list_url = re.sub(r'n\.pdf$', 'd.pdf', temp_url)
                            item["distribution"] = f"{cleaned_text},RETAIL_LINK:{retailer_list_url}"
                        except Exception as e:
                            logger.error(f"Failed to generate retailer URL for {item['url']}: {e}")

    # 3.2 處理 HK 數據
    # 判斷查找字典是否為空，如果不為空則進行處理
    if hk_distribution_lookup:
        for item in myDictFinalList:
            if item.get("source") == "香港食物安全中心" and item.get("url", "").endswith(".pdf"):
                global_id = item.get("global_id")
                if not global_id:
                    continue # 如果沒有 global_id，跳過此項

                # 使用 global_id 查找並更新 distribution
                new_distribution = hk_distribution_lookup.get(global_id)
                if new_distribution:
                    # 數據處理：替換分隔符並清理字符串
                    cleaned_text = new_distribution.replace('", "', '\n')
                    cleaned_text = cleaned_text.strip().strip('"')
                    item["distribution"] = cleaned_text


    # [7.3.2] 处理RASFF數據 : title
    # 1. 創建查找字典
    rasff_title_lookup = {}
    for single_item_dict in rasff_title_dict_list:
        rasff_title_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "RASFF":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in rasff_title_lookup:
                new_title = rasff_title_lookup[current_id]
                item['title'] = new_title

    # [7.3.3] 處理FDA數據 : recycling_reason
    # 1. 創建查找字典
    fda_recycling_reason_lookup = {}
    for single_item_dict in fda_recycling_reason_dict_list:
        fda_recycling_reason_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "US FDA":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in fda_recycling_reason_lookup:
                new_reason = fda_recycling_reason_lookup[current_id]
                item['recycling_reason'] = new_reason
    
    # [7.3.4] 處理EFSA數據
    efsa_count = 0
    for item in myDictFinalList:
        if item.get("source") == "EFSA":
            item["distribution"] = "--"
            efsa_count += 1
    
    # [7.3.5] 處理WHO數據
    who_count = 0
    for item in myDictFinalList:
        if item.get("source") == "WHO":
            item["distribution"] = "--"
            who_count += 1

    # [7.3.6] 處理 Government of Canada 數據 : distribution
    # 1. 創建查找字典
    canada_distribution_lookup = {}
    for single_item_dict in canada_distribution_dict_list:
        canada_distribution_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "Government of Canada":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in canada_distribution_lookup:
                new_distribution = canada_distribution_lookup[current_id]
                item['distribution'] = new_distribution

    # [7.3.7] 處理 The Food Standards Australia New Zealand (FSANZ) 數據 : recycling_reason
    fsanz_recycling_reason_lookup = {}
    for single_item_dict in fsanz_recycling_reason_dict_list:
        fsanz_recycling_reason_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "FSANZ":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in fsanz_recycling_reason_lookup:
                new_reason = fsanz_recycling_reason_lookup[current_id]
                item['recycling_reason'] = new_reason

    # [7.3.8] 處理 The Food Standards Australia New Zealand (FSANZ) 數據 : distribution
    # 1. 創建查找字典
    fsanz_distribution_lookup = {}
    for single_item_dict in fsanz_distribution_dict_list:
        fsanz_distribution_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    # 這種方法不需要計數器，也不依賴順序
    for item in myDictFinalList:
        # 檢查 item 是否來源於 FSANZ
        if item.get("source") == "FSANZ":
            # 獲取當前 item 的 global_id
            current_global_id = item.get("global_id")
            new_distribution = fsanz_distribution_lookup.get(current_global_id)
            if new_distribution is not None:
                # 更新當前 item 的 "distribution" 字段
                item["distribution"] = new_distribution
            


    # [7.3.9] 處理 Ministry for Primary Industries (MPI) 數據 : 副標題 recycling_reason
    # 1. 創建查找字典
    mpi_recycling_reason_lookup = {}
    for reason_dict in mpi_recycling_reason_dict_list:
        mpi_recycling_reason_lookup.update(reason_dict) 

    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "NZ MPI":
            current_id = item.get("global_id")
            # 從查找字典中獲取 reason。如果ID不存在，返回 item 當前的值，避免不必要的修改
            found_reason = mpi_recycling_reason_lookup.get(current_id)
            if found_reason: # 確保 found_reason 不是 None 或空字串
                # 定義關鍵字
                keyword = "due to"
                # 查找關鍵字的起始位置
                keyword_index = found_reason.find(keyword)
                # 如果找到了關鍵字,就從該位置截取字符串,作為最終 MPI 的 recycling_reason
                if keyword_index != -1:
                    # 提取 keyword 後面的內容
                    item["recycling_reason"] = found_reason[keyword_index:].strip()
                else:
                    item["recycling_reason"] = found_reason
    

    # [7.3.10] 處理 Ministry for Primary Industries (MPI) 數據 : distribution
    # 1. 創建查找字典
    mpi_distribution_lookup = {}
    for single_item_dict in mpi_distribution_dict_list:
        mpi_distribution_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "NZ MPI":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in mpi_distribution_lookup:
                new_distribution = mpi_distribution_lookup[current_id]
                item['distribution'] = new_distribution

    # [7.3.11] 處理 Food Standards Agency 數據 : distribution
    # 1. 創建查找字典
    fsa_distribution_lookup = {}
    for single_item_dict in fsa_distribution_dict_list:
        fsa_distribution_lookup.update(single_item_dict)   
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "UK FSA":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in fsa_distribution_lookup:
                new_reason = fsa_distribution_lookup[current_id]
                item['distribution'] = new_reason

    # [7.3.12] 處理 Rappel Conso 數據 : distribution
    # 1. 創建查找字典
    rappel_conso_distribution_lookup = {}
    for single_item_dict in rappel_conso_distribution_dict_list:
        rappel_conso_distribution_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "Rappel Conso":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in rappel_conso_distribution_lookup:
                new_distribution = rappel_conso_distribution_lookup[current_id]
                item['distribution'] = new_distribution


    # [7.3.13] 處理 Rappel Conso 數據 : recycling_reason
    # 1. 創建查找字典
    rappel_conso_recycling_reason_lookup = {}
    for single_item_dict in rappel_conso_recycling_reason_dict_list:
        rappel_conso_recycling_reason_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "Rappel Conso":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in rappel_conso_recycling_reason_lookup:
                new_reason = rappel_conso_recycling_reason_lookup[current_id]
                item['recycling_reason'] = new_reason

    # [7.3.14] 處理 NSW Food Standards Authority 數據 : distribution
    # 1. 創建查找字典
    nsw_distribution_lookup = {}
    for single_item_dict in nsw_distribution_dict_list:
        nsw_distribution_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "NSW Food Authority":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in nsw_distribution_lookup:
                new_distribution = nsw_distribution_lookup[current_id]
                item['distribution'] = new_distribution

    # [7.3.15] 處理 NSW Food Standards Authority 數據 : recycling_reason
    # 1. 創建查找字典
    nsw_recycling_reason_lookup = {}
    for single_item_dict in nsw_recycling_reason_dict_list:
        nsw_recycling_reason_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "NSW Food Authority":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in nsw_recycling_reason_lookup:
                new_reason = nsw_recycling_reason_lookup[current_id]
                item['recycling_reason'] = new_reason


    # [7.3.16] 處理 消費者廳 數據 distribution ： 已經被映射為 Consumer Affairs Agency, Government of Japan
    # 1. 創建查找字典
    japan_distribution_lookup = {}
    for single_item_dict in japan_distribution_dict_list:
        japan_distribution_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "Consumer Affairs Agency, Government of Japan":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in japan_distribution_lookup:
                new_distribution = japan_distribution_lookup[current_id]
                item['distribution'] = new_distribution

    # [7.3.17] 處理 消費者廳 數據 recycling_reason ： 已經被映射為 Consumer Affairs Agency, Government of Japan
    japan_recycling_reason_lookup = {}
    # 1. 創建查找字典
    for single_item_dict in japan_recycling_reason_dict_list:
        japan_recycling_reason_lookup.update(single_item_dict)
    # 2. 遍歷並更新 myDictFinalList
    for item in myDictFinalList:
        if item.get("source") == "Consumer Affairs Agency, Government of Japan":
            # 獲取當前項目的 global_id
            current_id = item.get('global_id')
            # 檢查這個 global_id 是否存在於查找字典中
            if current_id in japan_recycling_reason_lookup:
                new_reason = japan_recycling_reason_lookup[current_id]
                item['recycling_reason'] = new_reason


    # [7.4] 為myDictFinalList中的每個字典都添加 is_or_not_reason鍵
    # 由於 myDictFinalList 與 globalId_isOrNot_dict_list 都有對應的 globalId,因此可以先創建一個 globalId 到 is_or_not_reason 的映射字典
    globalid_to_reason = {item['globalId']: item['is_or_not_reason'] for item in globalId_isOrNot_dict_list}
    for item in myDictFinalList:
        item['is_or_not_reason'] = globalid_to_reason.get(item['global_id'], '未知')  # 使用get方法以防global_id不存在

    return myDictFinalList

async def createReport(data, userId):
    # ----- 在最開始進行舊文件的清理 -----
    # 創建需要的目錄
    logger.info("開始清理舊文件 ./data")
    images_dir_for_clean = "data/images"
    data_dir_for_clean = os.path.dirname(images_dir_for_clean)
    converted_images_dir_for_clean = os.path.join(data_dir_for_clean, "converted_images")
    pdf_files_dir_for_clean = os.path.join(data_dir_for_clean, "pdf_files")
    pdf_files_from_fsis_fsa_dir_for_clean = ("data/pdf_files_from_fsis_fsa")
    pdf_images_ocr_dir_for_clean = ("data/pdf_images_ocr")

    for directory in [
        images_dir_for_clean, converted_images_dir_for_clean, pdf_files_dir_for_clean, 
        pdf_files_from_fsis_fsa_dir_for_clean, pdf_images_ocr_dir_for_clean]:
        os.makedirs(directory, exist_ok=True)

    # 清理舊文件
    base_paths = [
        images_dir_for_clean, converted_images_dir_for_clean, pdf_files_dir_for_clean, 
        pdf_files_from_fsis_fsa_dir_for_clean, pdf_images_ocr_dir_for_clean]
    file_patterns = ['.jpg', '.png', '.pdf']
    clean_old_files(base_paths, file_patterns)
    logger.info("完成清理舊文件 ./data")
    
    # [8] 創建word報告生成服務
    userId = data.get("userId","test")
    start_time = time.time()
    try:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        
        
        myDictFinalList = await create_json(data, userId)
        # print("myDictFinalList:::",myDictFinalList)

        # ----------- 判斷回收原因是否需要翻譯 -------------
        # 提取source為"消費者廳"的recycling_reason,注意此時的 "消費者聽" 已經被映射為 "Consumer Affairs Agency, Government of Japan"
        # japan_recycling_reason_list = [item['recycling_reason'] for item in myDictFinalList if item.get("source") == "Consumer Affairs Agency, Government of Japan"]
        # 提取source為"香港食物安全中心"的recycling_reason,且url是不以".pdf"結尾的
        hk_recycling_reason_list = [item['recycling_reason'] for item in myDictFinalList if item.get("source") == "香港食物安全中心" and not item.get("url").endswith(".pdf")]

        # 如果 japan_recycling_reason_list 或 hk_recycling_reason_list 不為空列表,則 translate_flag 為 true,表示需要上傳至Dify進行翻譯
        # if japan_recycling_reason_list or hk_recycling_reason_list:
        if hk_recycling_reason_list:
            translate_flag = "true"
            
            if translate_flag == "true":
                max_retries = 3
                retry_count = 0
                retry_delay = 2
                logger.info(f"進行recycling_reason的翻譯,執行工作流......")
                while retry_count < max_retries:
                    try:
                        api_url = API_WORKFLOW_RUN_URL_PRO
                        api_key = API_KEY_PRO_V2  # workflow : foodsafety - translate
                        headers = {
                            'Authorization': f'Bearer {api_key}',
                            'Content-Type': 'application/json'
                        }

                        # 構建json_data
                        if hk_recycling_reason_list:
                            json_data = {
                                'inputs': {
                                    "translate_flag":translate_flag,
                                    "hk_recycling_reason_list": str(hk_recycling_reason_list),
                                    'sys.files': []
                                },
                                'response_mode': 'blocking',
                                'user': userId,
                            }
                        
                        # 如果兩個列表都不為空,則將兩個列表都上傳至Dify進行翻譯
                        # if japan_recycling_reason_list and hk_recycling_reason_list:
                        #     json_data = {
                        #         'inputs': {
                        #             "translate_flag":translate_flag,
                        #             "hk_recycling_reason_list": str(hk_recycling_reason_list),
                        #             # "japan_recycling_reason_list": str(japan_recycling_reason_list),
                        #             'sys.files': []
                        #         },
                        #         'response_mode': 'blocking',
                        #         'user': userId,
                        #     }
                        # # 如果只有消費者廳的recycling_reason列表不為空,則將其上傳至Dify進行翻譯
                        # elif japan_recycling_reason_list and not hk_recycling_reason_list:
                        #     json_data = {
                        #         'inputs': {
                        #             "translate_flag":translate_flag,
                        #             "japan_recycling_reason_list": str(japan_recycling_reason_list),
                        #             'sys.files': []
                        #         },
                        #         'response_mode': 'blocking',
                        #         'user': userId,
                        #     }
                        # # 如果只有香港食物安全中心的recycling_reason列表不為空,則將其上傳至Dify進行翻譯
                        # elif not japan_recycling_reason_list and hk_recycling_reason_list:
                        #     json_data = {
                        #         'inputs': {
                        #             "translate_flag":translate_flag,
                        #             "hk_recycling_reason_list": str(hk_recycling_reason_list),
                        #             'sys.files': []
                        #         },
                        #         'response_mode': 'blocking',
                        #         'user': userId,
                        #     }

                        response = requests.post(url=api_url, headers=headers, json=json_data)
                        if response.status_code == 200:
                            response_data = response.json()
                            # japan_recycling_reason_list = response_data.get("data", {}).get("outputs", {}).get("japan_recycling_reason_list", "")
                            hk_recycling_reason_list = response_data.get("data", {}).get("outputs", {}).get("hk_recycling_reason_list", "")
                            break
                        else:
                            # logger.warning(f"執行工作流`日本消費者廳食品回收原因翻譯`失敗，狀態碼: {response.status_code}，重試次數: {retry_count + 1}/{max_retries} - ID: {global_id}")
                            retry_count += 1
                            if retry_count < max_retries:
                                time.sleep(retry_delay)
                            if retry_count >= max_retries:
                                # japan_recycling_reason_list = []
                                hk_recycling_reason_list = []

                    except Exception as api_error:
                        # logger.error(f"執行工作流`日本消費者廳食品回收原因翻譯`時出錯: {str(api_error)}，重試次數: {retry_count + 1}/{max_retries} - ID: {global_id}")
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(retry_delay)
                        if retry_count >= max_retries:
                            # japan_recycling_reason_list = []
                            hk_recycling_reason_list = []

        # 處理來源為"消費者廳"的recycling_reason的翻譯
        # japan_translate_count = 0
        # for item in myDictFinalList:
        #     # 此時的 "消費者聽" 已經被映射為 "Consumer Affairs Agency, Government of Japan"
        #     if item.get("source") == "Consumer Affairs Agency, Government of Japan":  
        #         if japan_translate_count < len(japan_recycling_reason_list):
        #             item["recycling_reason"] = japan_recycling_reason_list[japan_translate_count]
        #         japan_translate_count += 1
        
        # 處理來源為"香港食物安全中心"的recycling_reason的翻譯
        hk_translate_count = 0
        for item in myDictFinalList:
            if item.get("source") == "香港食物安全中心" and not item.get("url").endswith(".pdf"):
                if hk_translate_count < len(hk_recycling_reason_list):
                    item["recycling_reason"] = hk_recycling_reason_list[hk_translate_count]
                hk_translate_count += 1

        # word報告輸出的模板文件
        TEMPLATE_PATH = 'report_template.docx'
        if not os.path.exists(TEMPLATE_PATH):
            raise Exception(f"模板文件 {TEMPLATE_PATH} 不存在")
        
        tpl = DocxTemplate(TEMPLATE_PATH)
        context = {"foodrecall_items": []}
        logger.info("開始處理數據項,准備生成食品回收報告服務......")
        
        MAX_WIDTH = Mm(40)  
        MAX_HEIGHT = Mm(40) 

        for idx, item in enumerate(myDictFinalList, 1):
            try:
                item_dict = item.copy()
                global_id = item_dict.get("global_id", "unknown")
                
                required_keys = ['title', 'url', 'recycling_reason']
                for key in required_keys:
                    if key not in item_dict:
                        logger.warning(f"項目缺少 {key} 鍵，設置默認值為'--' - ID: {global_id}")
                        item_dict[key] = '--'


                rich_text = RichText()
                original_title = "--" # Initialize to avoid UnboundLocalError
                
                # 對於不同的來源，定義不同的title處理方式
                if item_dict['title'].strip(): 
                    # CDPH : cdph_title_list
                    if item_dict.get("source") == "US CDPH":
                        original_title = f"{item_dict['title']}"
                    # 香港食物安全中心 : 不論來源當中是否是PDF的HK,title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "香港食物安全中心":
                        original_title = f"{item_dict['title']}"
                    # FSANZ : getData()拿到的title與fsanz_recycling_reason_dict_list進行合並
                    elif item_dict.get("source") == "FSANZ" and item_dict.get("recycling_reason") != "--":
                        original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                    # FSIS : title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "US FSIS":
                        original_title = f"{item_dict['title']}"
                    # FSA : title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "UK FSA" :
                        original_title = f"{item_dict['title']}"
                    # FSS : title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "FSS":
                        original_title = f"{item_dict['title']}"
                    # Government of Canada : title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "Government of Canada":
                        original_title = f"{item_dict['title']}"
                    # MPI : etData()拿到的title與mpi_recycling_reason_dict_list進行合並
                    elif item_dict.get("source") == "NZ MPI" and item_dict.get("recycling_reason") != "--":
                        original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                    # FDA : getData()拿到的title與fda_recyclig_reason_dict_list進行合並
                    elif item_dict.get("source") == "US FDA":
                        if item_dict.get("recycling_reason") != "--":
                            original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                        else:
                            original_title = f"{item_dict['title']}"
                    # RASFF : rasff_title_dict_list -> 根據匹配的 `rasff_pattern` 拿到title
                    elif item_dict.get("source") == "RASFF":
                        original_title = f"{item_dict['title']}"
                    # EFSA : title為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "EFSA":
                        original_title = f"{item_dict['title']}"
                    # WHO : ttitle為原始的getData()的title,不做處理
                    elif item_dict.get("source") == "WHO":
                        original_title = f"{item_dict['title']}"
                    # Rappel Conso : getData()拿到的title與rappel_conso_recycling_reason_dict_list進行合並
                    elif item_dict.get("source") == "Rappel Conso" and item_dict.get("recycling_reason") != "--":
                        original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                    # NSW Food Authority : getData()拿到的title與nsw_recycling_reason_dict_list進行合並   
                    elif item_dict.get("source") == "NSW Food Authority" and item_dict.get("distribution") != "--":
                        original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                    else:
                        # 對於其他來源，如果標題中已經包含食品回收原因，則直接使用原標題,否則則將標題與食品回收原因進行合並
                        if item_dict.get("is_or_not_reason") in "是" or item_dict['recycling_reason'] == "--":
                            original_title = item_dict['title']
                        else:
                            original_title = f"{item_dict['title']} + {item_dict['recycling_reason']}"
                    
                    try:
                        rich_text.add(original_title, url_id=tpl.build_url_id(item_dict['url']), color="#4472C4", underline=True)
                    except Exception as url_error:
                        logger.error(f"Title添加URL超鏈接時出錯: {str(url_error)} - ID: {global_id}")
                        rich_text.add(original_title, color="#4472C4", underline=True)
                
                else:
                    rich_text.add("--")
                item_dict['title'] = rich_text

                images = []
                images_dir = os.path.join(data_dir, "images")
                converted_dir = os.path.join(data_dir, "converted_images")
                os.makedirs(converted_dir, exist_ok=True)
                
                try:
                    # 首先檢查原始圖片目錄
                    # 1. 找到所有圖片
                    pngs = glob.glob(os.path.join(images_dir, f"{global_id}_*.png"))
                    jpgs = glob.glob(os.path.join(images_dir, f"{global_id}_*.jpg"))
                    # 2. 修改排序方式：提供一個 'key' 來自訂排序規則
                    # 為什麼要排序 : 
                    # 將其修改為 * 就能找到所有相關檔案。但更重要的一步是正確排序，否則你拿到的 "前15張" 可能會是 _0, _1, _10, _11, ... _14，而漏掉了 _2 到 _9。
                    all_imgs_sorted = sorted(
                        pngs + jpgs,
                        key=lambda f: int(os.path.splitext(os.path.basename(f))[0].split('_')[-1])
                    )
                    # 3. 從排好序的列表中選取前 15 個
                    all_imgs = all_imgs_sorted[:15]
                    
                    if all_imgs:
                        logger.info(f"為項目 {global_id} 找到 {len(all_imgs)} 個圖片文件")
                        
                        for img_path in all_imgs:
                            try:
                                # 檢查是否已有對應的轉換圖片
                                img_basename = os.path.basename(img_path)
                                converted_name = f"converted_{img_basename}"
                                converted_path = os.path.join(converted_dir, converted_name)
                                
                                if not os.path.exists(converted_path):
                                    # 如果轉換後的圖片不存在，則進行轉換
                                    converted_path = validate_and_convert_image(img_path, converted_dir)
                                
                                if converted_path and os.path.exists(converted_path):
                                    # 計算圖片自適應尺寸
                                    try:
                                        # 使用 Pillow Image 讀取圖片原始尺寸
                                        with Image.open(converted_path) as im:
                                            original_width_px, original_height_px = im.size

                                        # 計算長寬比
                                        if original_width_px == 0: continue # 避免除以零
                                        aspect_ratio = original_height_px / original_width_px

                                        # 假設以最大寬度為準，計算對應高度
                                        target_width = MAX_WIDTH
                                        target_height = target_width * aspect_ratio

                                        # 檢查計算出的高度是否超標
                                        if target_height > MAX_HEIGHT:
                                            # 如果高度超標，則以最大高度為準，重新計算寬度
                                            target_height = MAX_HEIGHT
                                            target_width = target_height / aspect_ratio
                                        
                                        # 創建 InlineImage，只需傳入 width，高度會自動按比例縮放
                                        img = InlineImage(tpl, converted_path, width=target_width)
                                        images.append(img)
                                        logger.info(f"成功添加圖片 (自適應尺寸): {converted_path}")

                                    except Exception as img_size_error:
                                        logger.error(f"計算圖片自適應尺寸時出錯 {converted_path}: {img_size_error}")
                                        # 如果計算出錯，直接使用固定寬度
                                        img = InlineImage(tpl, converted_path, width=Mm(40))
                                        images.append(img)
                                else:
                                    logger.warning(f"圖片轉換失敗或不存在: {img_path}")
                                    
                            except Exception as img_error:
                                logger.error(f"處理圖片時出錯: {img_path} - {str(img_error)}")
                                continue
                    else:
                        logger.warning(f"未找到項目 {global_id} 的任何圖片文件")
                            
                except Exception as e:
                    logger.error(f"搜索圖片時出錯: {str(e)} - ID: {global_id}")
                
                item_dict['products'] = images if images else "--"
                
                for key in ['source', 'distribution']:
                    if key not in item_dict:
                        item_dict[key] = '--'
                

                # distribution 超鏈接的使用
                distribution_text = item_dict.get('distribution', '')
                # 處理 MPI 的 RETAIL_LINK
                if '§HYPERLINK§' in distribution_text:
                    rich_dist_text = RichText()
                    lines = distribution_text.split('\n')
                    for i, line in enumerate(lines):
                        if '§HYPERLINK§' in line:
                            parts = line.split('§HYPERLINK§')
                            rich_dist_text.add(parts[0])
                            for part in parts[1:]:
                                link_components = part.split('§', 2)
                                if len(link_components) == 3:
                                    link_text, link_url, remaining_text = link_components
                                    try:
                                        rich_dist_text.add(link_text, url_id=tpl.build_url_id(link_url), color="#4472C4", underline=True)
                                    except Exception as url_error:
                                        logger.error(f"Distribution Hyperlink 添加URL時出錯: {url_error}")
                                        rich_dist_text.add(f"{link_text} (link error)")
                                    rich_dist_text.add(remaining_text)
                                else:
                                    rich_dist_text.add(part)
                        else:
                            rich_dist_text.add(line)
                        
                        if i < len(lines) - 1:
                            rich_dist_text.add('\n')
                    item_dict['distribution'] = rich_dist_text

                # 處理 CDPH 的 RETAIL_LINK
                elif 'RETAIL_LINK:' in distribution_text:
                    parts = distribution_text.split(',RETAIL_LINK:')
                    original_text = parts[0]
                    retail_url = parts[1]
                    
                    rich_text = RichText()
                    rich_text.add(f"{original_text}: ")
                    
                    try:
                        rich_text.add("Retail Distribution List", url_id=tpl.build_url_id(retail_url), color="#4472C4", underline=True)
                    except Exception as url_error:
                        logger.error(f"Distribution RETAIL_LINK 添加URL時出錯: {str(url_error)}")
                        rich_text.add("Retail Distribution List (link error)", color="#FF0000")
                    item_dict['distribution'] = rich_text

                # Case 3 (重要): 處理所有其他沒有特殊標記的普通文本
                else:
                    # 無需做任何操作，item_dict['distribution'] 已包含正確的純文本字符串。
                    # 為了代碼清晰，我們可以明確地賦值，但這不是必須的。
                    item_dict['distribution'] = distribution_text

                context["foodrecall_items"].append(item_dict)

                logger.info(f"[{idx}/{len(myDictFinalList)}]  成功添加數據項 - ID: {global_id}")
                logger.info(f"    └─ 標題: {original_title}")
                
            except Exception as e:
                logger.error(f"處理項目時出錯: {str(e)} - ID: {global_id if 'global_id' in locals() else 'unknown'}")
                fallback_dict = {
                    'title': RichText('Error processing item'),
                    'url': '--',
                    'source': '--',
                    'distribution': '--',
                    'recycling_reason': '--',
                    'products': '--'
                }
                context["foodrecall_items"].append(fallback_dict)
                continue

        date_str = datetime.now().strftime("%Y%m%d%H%M")
        filename = "report_{}_{}.docx".format(date_str,userId) 
        output_path = os.path.join(data_dir, filename)

        logger.info("開始渲染Word文檔")
        tpl.render(context)
        tpl.save(output_path)
        logger.info(f"Word文檔已保存至: {output_path}")
        
    except Exception as e:
        logger.error(f"生成報告時發生錯誤: {str(e)}")
        raise
    
    logger.info(f"生成報告時間: {time.time() - start_time} 秒")
    return filename


if __name__ == "__main__":
    data = {
    
    "globalIds":[
        "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa",
        "bmV3c0A2ODYyYjgxOGRjODY1ODdhNDE3ZTIzMzNAMjAyNS0wNi0yNlQxNjowMDowMC4wMDBa",
        "bmV3c0A2ODQwNmQ4ZmM0YjY4NGFkMjVlOGMwMDBAMjAyNS0wNi0wM1QxNjowMDowMC4wMDBa",
        "bmV3c0A2NzA1MzI5MDRAMjAyNS0wNi0zMFQxMjoxNDowMC4wMDBa",
        "bmV3c0A2Njk3OTEzMTNAMjAyNS0wNi0yN1QwOTozMjowMC4wMDBa",
        "bmV3c0A2NjkyNDEyNDhAMjAyNS0wNi0yNVQxMDoxNDowMC4wMDBa",
        "bmV3c0A2NjU2NzUyOTlAMjAyNS0wNi0xMlQwODo1NjowMC4wMDBa",
        "bmV3c0A2ODZiN2ViNTM2M2RhNTIxMWE3NjBhODVAMjAyNS0wNy0wNlQxNjowMDowMC4wMDBa",
        "bmV3c0A2ODY3NDgyYWQ3YzhiZWY5ZjcwYmVmNDlAMjAyNS0wNy0wM1QxNjowMDowMC4wMDBa",
        "bmV3c0A2NzE5OTYzNTRAMjAyNS0wNy0wNlQxNTowMDowMC4wMDBa",
        "bmV3c0A2NzEwMDI0NTdAMjAyNS0wNy0wM1QwMDoyMDowMC4wMDBa",
        "bmV3c0A2NzEwNzkzMzFAMjAyNS0wNy0wMlQxNTowMDowMC4wMDBa",
        "bmV3c0A2ODY3NTNjOGQ3YzhiZWE0NzAwYmZjOWNAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa",
        "bmV3c0A2ODY3NTNjZDc1MzM4ZWQ2MThjZmY1ZTJAMjAyNS0wNi0yNFQxNjowMDowMC4wMDBa",
        "bmV3c0A2ODY3NTNjZmYxY2JjZGUyOGNlMzgyNjFAMjAyNS0wNi0xOVQxNjowMDowMC4wMDBa",
        "d2Vic2l0ZUA2ODRkOWNhODIxNmJmZDBmNDM1MDdiNmRAMjAyNS0wNi0xM1QwNjozMDozNC4wMDBa",
        "d2Vic2l0ZUA2ODY1ZTU2MDM3Njc1MjI3Mjg4ZmYxZWNAMjAyNS0wNi0yNVQwMjoyNDowOS4wMDBa",
        "bmV3c0A2ODhkM2M3YmNiMTI1OWYzNzk3MzBhNzVAMjAyNS0wNy0yOFQxNjowMDowMC4wMDBa"
        
        
    ],
    "imagesByGlobalId":{

        "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa":[
            "https://www.mpi.govt.nz/assets/On-page-images/Food-Recalls/2025/June/Woori-Kimchi-brand-Minced-Raw-Garlic.jpg"
        ],
        
        "bmV3c0A2ODYyYjgxOGRjODY1ODdhNDE3ZTIzMzNAMjAyNS0wNi0yNlQxNjowMDowMC4wMDBa":[
            
        ],
        "bmV3c0A2ODQwNmQ4ZmM0YjY4NGFkMjVlOGMwMDBAMjAyNS0wNi0wM1QxNjowMDowMC4wMDBa":[
            "https://www.fsai.ie/getmedia/9ed6d37c-6538-4ef0-8f91-5f127815e73b/photos-for-dressings.png?width=816&height=452&ext=.png"


        ],
        "bmV3c0A2NzA1MzI5MDRAMjAyNS0wNi0zMFQxMjoxNDowMC4wMDBa":[
            "https://www.fda.gov/files/styles/recall_image_small/public/image_1_101.png",
            "https://www.fda.gov/files/styles/recall_image_small/public/image_2_71.png",
            "https://www.fda.gov/files/styles/recall_image_small/public/image_3_69.jpg"
        ],
        "bmV3c0A2Njk3OTEzMTNAMjAyNS0wNi0yN1QwOTozMjowMC4wMDBa":[
            "https://www.fda.gov/files/styles/recall_image_small/public/res_97140-1.png",
            "https://www.fda.gov/files/styles/recall_image_small/public/res_97140-2.png"
        ],
        "bmV3c0A2NjkyNDEyNDhAMjAyNS0wNi0yNVQxMDoxNDowMC4wMDBa":[
            "https://www.fda.gov/files/styles/recall_image_small/public/res_97152-1.jpg",
            "https://www.fda.gov/files/styles/recall_image_small/public/res_97152-2.jpg"
        ],
        "bmV3c0A2NjU2NzUyOTlAMjAyNS0wNi0xMlQwODo1NjowMC4wMDBa":[
            
        ],
        "bmV3c0A2ODZiN2ViNTM2M2RhNTIxMWE3NjBhODVAMjAyNS0wNy0wNlQxNjowMDowMC4wMDBa":[
 
        ],
        "bmV3c0A2ODY3NDgyYWQ3YzhiZWY5ZjcwYmVmNDlAMjAyNS0wNy0wM1QxNjowMDowMC4wMDBa":[

        ],
        "bmV3c0A2NzE5OTYzNTRAMjAyNS0wNy0wNlQxNTowMDowMC4wMDBa":[

        ],
        "bmV3c0A2NzEwMDI0NTdAMjAyNS0wNy0wM1QwMDoyMDowMC4wMDBa":[

        ],
        "bmV3c0A2NzEwNzkzMzFAMjAyNS0wNy0wMlQxNTowMDowMC4wMDBa":[
            
        ],
        "bmV3c0A2ODY3NTNjOGQ3YzhiZWE0NzAwYmZjOWNAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa":[

        ],
        "bmV3c0A2ODY3NTNjZDc1MzM4ZWQ2MThjZmY1ZTJAMjAyNS0wNi0yNFQxNjowMDowMC4wMDBa":[

        ],
        "bmV3c0A2ODY3NTNjZmYxY2JjZGUyOGNlMzgyNjFAMjAyNS0wNi0xOVQxNjowMDowMC4wMDBa":[

        ],
        "d2Vic2l0ZUA2ODRkOWNhODIxNmJmZDBmNDM1MDdiNmRAMjAyNS0wNi0xM1QwNjozMDozNC4wMDBa":[
           
        ],
        "d2Vic2l0ZUA2ODY1ZTU2MDM3Njc1MjI3Mjg4ZmYxZWNAMjAyNS0wNi0yNVQwMjoyNDowOS4wMDBa":[

        ],
        "bmV3c0A2ODhkM2M3YmNiMTI1OWYzNzk3MzBhNzVAMjAyNS0wNy0yOFQxNjowMDowMC4wMDBa":[
            
        ]
        
        
    },

    "userId":"test0807_"
}
    asyncio.run(create_json(data,userId="test"))
