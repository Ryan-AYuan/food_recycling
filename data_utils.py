import re
import httpx
import logging
import requests
from typing import List, Dict
import os
import time

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getData(globalIds):
    """
    獲取原始數據 ： 食品召回產品召回條目

    Args:
        globalIds: 傳入帖子的ID,可以傳入列表

    Returns:
        以列表的形式返回原始數據:
            [
                {
                    "globalId": "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa",
                    "url": "https://www.mpi.govt.nz/assets/On-page-images/Food-Recalls/2025/June/Woori-Kimchi-brand-Minced-Raw-Garlic.jpg",
                    "from": "The Ministry for Primary Industries (MPI)",
                    "title": "Woori Kimchi brand Minced Raw Garlic",
                    ......
                },
                
            ]  
    """
    req = httpx.post(
        url="http://api.ersinfotech.com/helper-api/egraphql",
        json={
            "docid": "6836d0c544c8650f3e66334c",
            "variables": {
                "globalId": globalIds
            }
        })

    res_data = req.json()["data"]["searchByGlobalId"]
    logger.info(f" 原始數據提取成功！")
    return res_data

def create_product_dict(data, raw_data):
    """
    將原始產品召回數據轉換為自定義的字典

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

        raw_data (list): `getData()` 函數返回的 JSON 格式的每個食品召回產品召回條目列表
            [
                {
                    "globalId": "bmV3c0A2NzA4NDQ5MTFAMjAyNS0wNy0wMVQxNjowMDowMC4wMDBa",
                    "url": "https://www.mpi.govt.nz/assets/On-page-images/Food-Recalls/2025/June/Woori-Kimchi-brand-Minced-Raw-Garlic.jpg",
                    "from": "The Ministry for Primary Industries (MPI)",
                    "title": "Woori Kimchi brand Minced Raw Garlic",
                    ......
                },
                
            ]   
        
    Returns:
        dict：具有以下鍵的結構化字典：
            - globalIds：所有全局帖子 ID 的列表
            - imagesByGlobalId：將全局 ID 映射到圖像 URL 的字典
            - urlDict：將全局 ID 映射到產品 URL 的字典
            - fromDict： 將全局 ID 映射到源國的 dict
            - titleDict： 將全局 ID 映射到產品標題的字典
    """
    myDict = {
        "globalIds": [],
        "imagesByGlobalId": {},
        "urlDict": {},
        "fromDict": {},
        "titleDict": {}
    }

    myDict["globalIds"].extend(data["globalIds"])
    myDict["imagesByGlobalId"].update(data["imagesByGlobalId"])

    for item in raw_data:
        global_id = item["globalId"]
        myDict["urlDict"][global_id] = item.get("url","")
        myDict["fromDict"][global_id] = item.get("from","")
        myDict["titleDict"][global_id] = item.get("title","")

    return myDict


def add_field(myDict_list, distribution_list, recycling_reason_list):
    """
    將 Dify工作流得到的 distribution_list 、 recycling_reason_list 的值添加到 myDict_list 中的每個字典,並確保 "distribution" 字段位於 "products" 字段之前

    Args:
        myDict_list (list): 數據列表，包含多個字典
        distribution_list (list): 要添加的分發地區列表

    Returns:
        list: 更新後的數據列表
    """
    myDict_list_add_distribution_recycling_reason = []
    if len(myDict_list) != len(distribution_list) or len(myDict_list) != len(recycling_reason_list):
        raise ValueError("myDict_list 和 distribution_list、recycling_reason_list 必須具有相同的長度!distribution字段添加失敗!")

    for i, item in enumerate(myDict_list):
        new_item = item.copy()
        new_item["distribution"] = distribution_list[i]
        new_item["recycling_reason"] = recycling_reason_list[i]

        ordered_item = {
            "num": new_item["num"],
            "title": new_item["title"],
            "url": new_item["url"],
            "source": new_item["source"],
            "distribution": new_item["distribution"],
            "recycling_reason": new_item["recycling_reason"],
            "products": new_item["products"],
            "global_id":new_item["global_id"]
        }
        myDict_list_add_distribution_recycling_reason.append(ordered_item)

    return myDict_list_add_distribution_recycling_reason


def transform_mydict_to_mydict_list_final(
        myDict,
        distribution_list,
        recycling_reason_list,
        verbose = True
):
    """
    將 `create_product_dict` 函數返回的 myDict 字典轉換為最終的 myDict_list 結構,
    然後通過 add_field()函數,將 distribution_list 添加到 myDict_list 中；將 recycling_reason_list 添加到 myDict_list 中,
    最終得到 myDict_list_final

    Args:
        myDict (dict): create_product_dict()函數返回的自定義字典myDict
        distribution_list (list): Dify 生成的 distribution_list,後續將用來添加到 myDictFinal_list 的 "distribution" 字段
        recycling_reason_list (list): Dify 生成的 recycling_reason_list,後續將用來添加到 myDictFinal_list 的 "recycling_reason" 字段
        verbose (bool): 是否打印詳細日誌 參數控制日誌輸出

    Returns:
        list: 指定 myDict_list 格式的字典列表
    """
    source_mapping = {
        "The Food Safety Authority of Ireland (FSAI)": "FSAI",
        "The Food Standards Australia New Zealand (FSANZ)": "FSANZ",
        "Ministry for Primary Industries (MPI)": "NZ MPI",
        "Government of Canada": "Government of Canada",
        "Food Standards Agency": "UK FSA",
        "The US Food and Drug Administration (FDA)": "US FDA",
        "The USDA Food Safety and Inspection Service (FSIS)": "US FSIS",
        "California Department of PublicHealth (CDPH)": "US CDPH",
        "Food Standards Scotland (FSS)": "FSS",
        "Food and Agriculture Organization of the United Nations (FAO)": "FAO",
        "World Organisationfor Animal Health (WOAH)": "WOAH",
        "World Health Organization (WHO)": "WHO",
        "Alim'Agri pour Alimentation et Agriculture (The French Ministry of Agriculture and Food)": "The French Ministry of Agriculture and Food",
        "European food safety authority (EFSA)": "EFSA",
        "Australian Competition & Consumer Commission (ACCC)": "ACCC",
        "The Michigan Department of Agriculture and Rural Development (MDARD)": "MDARD",
        "Oregon Health Authority (OHA)": "OHA",
        "The Canadian Food Inspection Agency (CFIA)": "CFIA",
        "消費者廳(Consumer Affairs Agency, Government of Japan)": "Consumer Affairs Agency, Government of Japan"
    }

    source_patterns = {
        "FSAI": ["food safety authority of ireland", "fsai"],
        "FSANZ": ["food standards australia", "fsanz"],
        "NZ MPI": ["ministry for primary industries", "mpi", "new zealand mpi"],
        "UK FSA": ["food standards agency", "fsa"],
        "US FDA": ["food and drug administration", "fda"],
        "US FSIS": ["food safety and inspection service", "fsis"],
        "US CDPH": ["california department of public health", "cdph"],
        "FSS": ["food standards scotland", "fss"],
        "FAO": ["food and agriculture organization of the united nations", "fao"],
        "WOAH": ["world organisationfor animal health", "woah"],
        "WHO": ["world health organization", "who"],
        "The French Ministry of Agriculture and Food": ["alim'agri pour alimentation et agriculture", "the french ministry of agriculture and food"],
        "EFSA": ["european food safety authority", "efsa"],
        "ACCC": ["australian competition & consumer commission", "accc"],
        "MDARD": ["michigan department of agriculture and rural development", "mdard"],
        "OHA": ["oregon health authority", "oha"],
        "CFIA": ["canadian food inspection agency", "cfia"],
        "Consumer Affairs Agency, Government of Japan": ["消費者廳", "consumer affairs agency, government of japan"]
    }

    def standardize_source(source):
        """標准化 source 字段"""

        # 空值處理
        if not source: 
            return source

        # 轉換為小寫比較    
        source_lower = source.lower()
        # 首先嘗試完全匹配
        for full_name, abbreviation in source_mapping.items():
            if full_name.lower() in source_lower:
                return abbreviation
            
        # 如果完全匹配失敗，則嘗試模式匹配
        for abbreviation, patterns in source_patterns.items():
            for pattern in patterns:
                if pattern in source_lower:
                    return abbreviation
                
        # 未找到匹配,返回原值
        return source

    myDict_list = [
        {
            "num": idx + 1,
            "title": myDict["titleDict"][global_id],
            "url": myDict["urlDict"][global_id],
            "source": standardize_source(myDict["fromDict"][global_id]),
            "products": [],
            "global_id":global_id
        }
        for idx, global_id in enumerate(myDict["globalIds"])
    ]

    # 將 distribution 字段、recycling_reason 字段 添加到 myDict_list 中
    final_data = add_field(myDict_list=myDict_list,
                          distribution_list=distribution_list,
                          recycling_reason_list=recycling_reason_list)

    # 處理 distribution 字段
    for item in final_data:
        distribution = item.get("distribution", "")
        if "全國" in distribution or "全國" in distribution:
            item["distribution"] = "National"

    return final_data

def html_to_markdown(html_content):
    """
    忽略鏈接,圖像等標簽定位,僅提取 `getData` 函數返回的 content 多行字符串文本中的文本內容,但保留PDF鏈接

    Args:
        html_content (str): 含有鏈接,圖像等標簽定位的 `getData` 函數返回的多行字符串文本

    Returns:
        str: content_cleaned (str): 不含鏈接(除PDF),圖像等標簽定位的,僅有內容的多行字符串文本
    """
    # 保存PDF鏈接
    pdf_links = []
    pdf_pattern = r'<p>.*?<a\s+href="([^"]*\.pdf)"[^>]*>([^<]*)</a>.*?</p>'
    for match in re.finditer(pdf_pattern, html_content, re.IGNORECASE | re.DOTALL):
        pdf_links.append(match.group(0))

    # 轉換HTML標簽
    for i in range(6, 0, -1):
        html = re.sub(rf'<h{i}>(.*?)</h{i}>', rf'{"#" * i} \1', html_content, flags=re.DOTALL)

    html = re.sub(r'<b>(.*?)</b>', r'**\1**', html, flags=re.DOTALL)
    html = re.sub(r'<strong>(.*?)</strong>', r'**\1**', html, flags=re.DOTALL)
    html = re.sub(r'<i>(.*?)</i>', r'*\1*', html, flags=re.DOTALL)
    html = re.sub(r'<em>(.*?)</em>', r'*\1*', html, flags=re.DOTALL)
    html = re.sub(r'<p>(?!.*?\.pdf.*?</p>)(.*?)</p>', r'\1\n', html, flags=re.DOTALL)
    html = re.sub(r'<br\s*/?>', r'\n', html, flags=re.DOTALL)
    html = re.sub(r'<ul>(.*?)</ul>', lambda m: re.sub(r'<li>(.*?)</li>', r'* \1', m.group(1), flags=re.DOTALL), html, flags=re.DOTALL)
    html = re.sub(r'<ol>(.*?)</ol>', lambda m: re.sub(r'<li>(.*?)</li>', lambda n, c=1: f'{c}. {n.group(1)}\n', m.group(1), flags=re.DOTALL), html, flags=re.DOTALL)
    html = re.sub(r'<(?!a\s+href="[^"]*\.pdf")[^>]*>', '', html)
    html = html.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&quot;', '"').replace('&#39;', "'")

    # 恢復PDF鏈接
    for pdf_link in pdf_links:
        if pdf_link not in html:
            html += f"\n{pdf_link}"

    return html.strip() 

def clean_old_files(base_paths: List[str], file_patterns: List[str], hours: int = 1):
    """
    定期清理指定目錄下的舊文件

    Args:
        base_paths: 需要清理的目錄列表
        file_patterns: 需要清理的文件类型列表
        hours: 文件保留时间（小时）
    """
    timestamp = time.time() - (hours * 3600)
    
    for base_path in base_paths:
        if not os.path.exists(base_path):
            continue
            
        files = os.listdir(base_path)
        for file in files:
            if any(file.endswith(pattern) for pattern in file_patterns):
                file_path = os.path.join(base_path, file)
                try:
                    if os.path.getctime(file_path) < timestamp:
                        os.remove(file_path)
                        logger.debug(f"已刪除舊文件: {file_path}")
                except Exception as e:
                    logger.error(f"刪除文件失敗 {file_path}: {str(e)}")