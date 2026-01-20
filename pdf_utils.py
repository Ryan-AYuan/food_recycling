import os
import time
import random
import logging
import fitz  # PyMuPDF
from PIL import Image
from curl_cffi import requests as cffi_requests
from pdf_image_extractor import PDFImageExtractor
from pdfminer.high_level import extract_text
import re
import aiohttp
import aiofiles
import shutil

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def download_pdf(url, output_filename):
    """
    從URL下載PDF文件並保存到本地 : 當前主要是用來下載來源為CDPH的PDF文件

    Args:
        url (str): PDF文件的URL
        output_filename (str): 保存PDF的文件名
        
    Returns:
        str/bool: 下載成功返回文件路徑，失敗返回False
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf,*/*'
        }
        
        logger.info(f"正在使用 requests 下載PDF: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, ssl=False) as response:
                if response.status != 200:
                    raise Exception(f"下載失敗，狀態碼: {response.status}")
                    
                content_type = response.headers.get('content-type', '')
                if 'pdf' not in content_type.lower() and not url.lower().endswith('.pdf'):
                    raise Exception(f"下載的不是PDF文件，content-type: {content_type}")
                    
                content = await response.read()
                async with aiofiles.open(output_filename, 'wb') as f:
                    await f.write(content)
                    
                logger.info(f"PDF文件已成功保存為: {output_filename}")
                return output_filename
        
    except Exception as e:
        logger.error(f"下載PDF時發生錯誤: {str(e)}")
        return False


async def download_pdf_for_fsis_and_fsa(url, output_filename):
    """
    下載來源為FSIS和FSA的內置圖片的PDF文件 : 使用 curl_cffi 從URL下載PDF文件，可以有效繞過TLS指紋檢測
    
    Args:
        url (str): PDF文件的URL
        output_filename (str): 保存PDF的文件名(路徑)
        
    Returns:
        str/bool: 下載成功返回PDF文件名的路徑,失敗則返回False
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if retry_count > 0:
                logger.info(f"\n第 {retry_count} 次重試下載...")
                wait_time = retry_count * 2 + random.uniform(1, 3)
                logger.info(f"等待 {wait_time:.1f} 秒後重試...")
                await asyncio.sleep(wait_time)
            
            logger.info(f"正在使用 curl_cffi 下載PDF: {url}")

            response = cffi_requests.get(
                url, 
                impersonate="chrome120",
                timeout=5
            )
            
            response.raise_for_status()
                
            async with aiofiles.open(output_filename, 'wb') as f:
                await f.write(response.content)
                
            logger.info(f"PDF文件已成功保存為: {output_filename}")
            return output_filename
            
        except Exception as e:
            retry_count += 1
            logger.error(f"下載時發生錯誤: {type(e).__name__} - {e}")
            if retry_count >= max_retries:
                logger.info(f"已達到最大重試次數({max_retries})，放棄下載。")
                return False
            else:
                logger.info("下載准備重試...")

async def process_pdf_with_extractor(global_id: str, pdf_url: str, pdf_dir: str, images_dir: str) -> bool:
    """
    如果最終的圖片文件不存在，則執行PDF的下載和轉換；如果圖片已存在，則跳過(不執行PDF下載,也不執行轉換)

    Args:
        global_id (str): 帖子ID(全局)，用於生成圖片文件名
        pdf_url (str): 需要下載的PDF文件對應的URL
        pdf_dir (str): 下載好的PDF文件保存目錄
        images_dir (str): 轉換後的PNG圖片保存目錄

    Returns:
        bool: 處理成功返回True，失敗返回False

    """
    try:
        os.makedirs(pdf_dir, exist_ok=True)
        os.makedirs(images_dir, exist_ok=True)

        # 檢查最終產物（第一張圖片）是否存在
        expected_image_path = os.path.join(images_dir, f"{global_id}_1.png")
        if os.path.exists(expected_image_path):
            logger.info(f"目標: {global_id}已存在,跳過相關PDF和圖片處理")
            return True

        # 如果圖片不存在，則繼續執行完整流程
        pdf_filename = os.path.join(pdf_dir, f"{global_id}.pdf")

        # 1. 檢查PDF是否存在，不存在則下載
        if not os.path.exists(pdf_filename):
            logger.info(f"PDF文件不存在，開始下載: {pdf_url}")
            if not await download_pdf_for_fsis_and_fsa(pdf_url, pdf_filename):
                logger.error(f"為 globalId {global_id} 下載PDF失敗。")
                return False
        else:
            logger.info(f"PDF文件已存在，跳過下載: {pdf_filename}")

        # 2. 調用 PDFImageExtractor 轉換圖片
        extractor = PDFImageExtractor(images_dir)
        pdf_url_local = f"file:///{os.path.abspath(pdf_filename).replace(os.sep, '/')}"
        
        # 轉換器
        results = extractor.process_pdf_urls([pdf_url_local])

        if results['success']:
            pdf_result = results['success'][0]

            # 3. 遍歷轉換後的圖片路徑並重命名
            for i, img_path in enumerate(pdf_result['image_paths'], 1):
                # 確保源文件存在
                if not os.path.exists(img_path):
                    logger.warning(f"轉換器聲稱成功，但找不到文件: {img_path}")
                    continue
                
                # 設置目標文件名和路徑
                new_filename = f"{global_id}_{i}.png"
                new_path = os.path.join(images_dir, new_filename)
                
                try:
                    # 如果源文件和目標文件是同一個文件，則跳過
                    if os.path.samefile(img_path, new_path):
                        logger.debug(f"源文件和目標文件相同，跳過移動: {img_path}")
                        continue
                        
                    # 如果目標文件已存在，先刪除
                    if os.path.exists(new_path):
                        os.remove(new_path)
                    
                    # 使用 shutil.move 進行重命名
                    shutil.move(img_path, new_path)
                except FileNotFoundError:
                    logger.warning(f"移動文件時出現錯誤，源文件可能已被移動: {img_path}")
                    continue
                except Exception as e:
                    logger.error(f"移動文件時出現未預期的錯誤: {str(e)}")
                    continue

            logger.info(f"成功為 {global_id} 轉換並重命名了 {len(pdf_result['image_paths'])} 張圖片。")
            return True
        else:
            # 錯誤信息記錄
            error_info = results.get('failed')
            if error_info:
                logger.error(f"處理PDF轉換圖片的過程失敗: {error_info[0].get('error', '未知錯誤')}")
            else:
                logger.error("處理PDF轉換圖片的過程失敗，且沒有返回詳細錯誤信息。")
            return False

    except Exception as e:
        logger.error(f"處理PDF轉換圖片時發生嚴重錯誤: {str(e)}", exc_info=True)
        return False

def convert_pdf_to_image(pdf_path, output_dir, output_format='jpg', dpi=200):
    """
    將單頁PDF文件轉換為圖片 : 來源為CDPH的文章，需要下載其PDF文件，並轉換為圖片，然後上傳到Dify進行OCR，識別到對應的distribution

    Args:
        pdf_path (str): 來源為CDPH的PDF文件的路徑
        output_dir (str): 輸出圖片的目錄路徑
        output_format (str): 輸出圖片格式，'jpg'或'png'
        dpi (int): 輸出圖片的DPI（每英寸點數）
        
    Returns:
        str: 生成的圖片文件路徑，如果轉換失敗則返回空字符串
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        pdf_document = fitz.open(pdf_path)
        
        if len(pdf_document) != 1:
            pdf_document.close()
            return ""
        
        zoom = dpi / 72
        page = pdf_document[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        
        output_path = os.path.join(output_dir, f"{pdf_name}.{output_format}")
        
        if output_format.lower() == 'jpg':
            pix.save(output_path, "jpeg")
        else:
            pix.save(output_path, "png")
        
        pdf_document.close()
        return output_path
        
    except Exception as e:
        return "" 
    
# ------- 提取PDF中的零售商信息 : 針對 CDPH 來源 (由於零售商信息過多,不宜展示在word報告中,暫時棄用;使用PDF對應的URL進行代替展示) -------
def clean_text(text):
    # 清理多余的空格和换行
    return ' '.join(text.split()).strip()

def is_valid_retailer(text):
    # 检查零售商名称的有效性
    invalid_words = ['page', 'confidential', 'updated', 'ca', 'worksheet', 'retailer', 'retail', 'location', 'address']
    return text and not any(word.lower() in text.lower() for word in invalid_words)

def is_valid_address(text):
    # 检查地址的有效性
    return bool(re.search(r'\d+.*?[A-Za-z]', text))

def extract_info_from_pdf(pdf_path):
    # 提取文本
    text = extract_text(pdf_path)
    
    # 将文本按行分割并清理
    lines = [clean_text(line) for line in text.split('\n') if clean_text(line)]
    
    # 存储结果
    results = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # 跳过表头和无关信息
        if any(header in line.lower() for header in ['retailer', 'retail location address', 'city']):
            i += 1
            continue
            
        # 如果找到一个可能的零售商名称
        if is_valid_retailer(line) and i + 2 < len(lines):
            retailer = line
            address = lines[i + 1]
            city = lines[i + 2]
            
            # 如果地址有效
            if is_valid_address(address):
                # 清理城市信息
                if 'CA' in city:
                    city = city.split('CA')[0].strip()
                
                # 清理电话号码
                city = re.sub(r'\d{3}-\d{3}-\d{4}', '', city).strip()
                retailer = re.sub(r'\d{3}-\d{3}-\d{4}', '', retailer).strip()
                
                # 确保所有字段都有效
                if retailer and address and city:
                    results.append((retailer, address, city))
        i += 1
    
    return results

def format_output(info_tuple):
    retailer, address, city = info_tuple
    # 确保每个字段都是清理过的
    retailer = clean_text(retailer)
    address = clean_text(address)
    city = clean_text(city)
    return f"{retailer} - {address} - {city}"

def process_pdf(pdf_file):
    try:
        info_tuples = extract_info_from_pdf(pdf_file)
        # 过滤掉可能的无效数据
        valid_tuples = [(r, a, c) for r, a, c in info_tuples 
                      if len(r) > 1 and len(a) > 1 and len(c) > 1
                      and is_valid_retailer(r)
                      and is_valid_address(a)]
        
        # 只返回前5条记录的格式化结果
        return [format_output(info) for info in valid_tuples[:5]]
    except Exception as e:
        print(f"处理文件 {pdf_file} 时出错: {str(e)}")
        return []
