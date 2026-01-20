import os
import random
import logging
import asyncio
import aiohttp
import aiofiles
from PIL import Image
from typing import List, Optional
from urllib.parse import urlparse

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def download_images_with_timestamp(
        myDict,
        images_dir="data/images",
        download_delay=3,
        verbose=True,
        max_retries=3,
        max_concurrent=5
):
    """
    异步下载图片并保存到指定目录，如果图片已存在则跳过下载
    增强的反反爬虫措施

    Args:
        myDict (dict): 自定义的myDict字典,包含图片URL的数据结构，格式如：
            {
                "globalIds": ["id1", "id2"],
                "imagesByGlobalId": {
                    "id1": ["url1", "url2"],
                    "id2": ["url3"]
                }
            }
        images_dir (str): 基础存储目录（默认当前路径下的images文件夹）
        download_delay (int): 下载间隔秒数（防封IP）
        verbose (bool): 是否打印详细日志
        max_retries (int): 下载失败时的最大重试次数
        max_concurrent (int): 最大并发下载数量
    Returns:
        list: 成功下载的文件路径列表
    """
    
    success_files = []
    failed_urls = []

    # User-Agent列表
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36"
    ]

    # Referer域名列表
    common_referers = [
        "https://www.fda.gov/safety/",
        "https://www.fsis.usda.gov/",
        "https://www.food.gov.uk/",
        "https://www.fsai.ie/",
        "https://www.foodstandards.gov.au/",
        "https://www.mpi.govt.nz/",
        "https://recalls-rappels.canada.ca/en/",
        "https://www.cdph.ca.gov/",
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://www.yahoo.com/",
        "https://duckduckgo.com/",
        "https://www.baidu.com/"
    ]

    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def download_single_image(session: aiohttp.ClientSession, 
                                  url: str, 
                                  filename: str, 
                                  global_id: str, 
                                  img_idx: int) -> Optional[str]:
        """下載單個圖片的異步函數"""
        # 檢查文件是否已存在且有效
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            if file_size > 0:
                logger.info(f"圖片已存在，跳過下載: {filename}")
                success_files.append(filename)
                return filename
        
        # 檢查是否存在其他格式的圖片（例如.jpg或.png）
        base_filename = os.path.splitext(filename)[0]
        for ext in ['.png', '.jpg']:
            alt_filename = base_filename + ext
            if os.path.exists(alt_filename):
                file_size = os.path.getsize(alt_filename)
                if file_size > 0:
                    logger.info(f"圖片已存在（其他格式），跳過下載: {alt_filename}")
                    success_files.append(alt_filename)
                    return alt_filename
                
        async with semaphore:
            download_success = False
            for retry in range(max_retries):
                try:
                    base_delay = download_delay
                    await asyncio.sleep(base_delay)

                    headers = {
                        "User-Agent": random.choice(user_agents),
                        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Site": "cross-site",
                        "Sec-Fetch-Mode": "no-cors",
                        "Sec-Fetch-Dest": "image",
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache"
                    }
                    
                    if retry == 0:
                        parsed_url = urlparse(url)
                        headers["Referer"] = f"{parsed_url.scheme}://{parsed_url.netloc}/"
                    else:
                        headers["Referer"] = random.choice(common_referers)

                    try:
                        async with session.get(url, 
                                             headers=headers, 
                                             timeout=aiohttp.ClientTimeout(total=30),
                                             ssl=False) as response:
                            if response.status == 200:
                                content_type = response.headers.get('content-type', '').lower()
                                if 'image' not in content_type and content_type != 'application/octet-stream':
                                    break

                                content = await response.read()
                                if len(content) > 0:
                                    async with aiofiles.open(filename, 'wb') as f:
                                        await f.write(content)

                                    file_size = os.path.getsize(filename)
                                    if file_size > 0:
                                        success_files.append(filename)
                                        download_success = True
                                        if verbose:
                                            pass
                                            # logger.info(f"[SUCCESS] 圖片已保存: {filename} ({file_size} bytes)")
                                        return filename
                                    else:
                                        logger.warning(f"下載的圖片為空: {filename}")
                                        if os.path.exists(filename):
                                            os.remove(filename)
                                else:
                                    logger.warning(f"響應內容為空,圖片下載失敗: {url}")

                            elif response.status == 403:
                                logger.warning(f"訪問被拒絕 (HTTP 403),圖片下載失敗: {url}")
                                await asyncio.sleep(base_delay)

                            elif response.status == 404:
                                logger.error(f"圖片不存在 (HTTP 404),圖片下載失敗: {url}")
                                break
                            else:
                                logger.warning(f"下載失敗 (HTTP {response.status}),圖片下載失敗: {url}")
                                await asyncio.sleep(base_delay)

                    except aiohttp.ClientError as e:
                        logger.error(f"請求錯誤,圖片下載失敗: {url} - {str(e)}")
                        await asyncio.sleep(1)

                except asyncio.TimeoutError:
                    logger.warning(f"請求超時,圖片下載失敗: {url}")
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"下載出錯,圖片下載失敗: {url} - {str(e)}")
                    await asyncio.sleep(1)

            if not download_success:
                logger.error(f"圖片最終下載失敗: {url}")
                failed_urls.append(url)
            return None

    async def process_all_images():
        """處理所有圖片下載的主異步函數"""
        connector = aiohttp.TCPConnector(limit=max_concurrent, force_close=True)
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(connector=connector, 
                                       timeout=timeout,
                                       trust_env=True) as session:
            tasks = []
            for idx, global_id in enumerate(myDict["globalIds"], start=1):
                image_urls = myDict["imagesByGlobalId"].get(global_id, [])
                for img_idx, url in enumerate(image_urls, start=1):
                    filename = os.path.join(images_dir, f"{global_id}_{img_idx}.png")
                    task = download_single_image(session, url, filename, global_id, img_idx)
                    tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful_downloads = [r for r in results if isinstance(r, str)]
            failed_downloads = len(results) - len(successful_downloads)
            
            if verbose:
                logger.info("\n" + "="*50)
                logger.info("[COMPLETE] 圖片下載任務處理完成！")
                # logger.info(f"-> 成功下載: {len(successful_downloads)} 個文件")
                # logger.info(f"-> 失敗下載: {failed_downloads} 個文件")
                #logger.info(f"-> 總計處理: {len(successful_downloads)}/{len(results)} 個文件")
                if failed_urls:
                    # logger.info("\n失敗的URL列表:")
                    for url in failed_urls:
                        # logger.info(f"- {url}")
                        pass
                logger.info("="*50 + "\n")

    await process_all_images()
    return success_files

def validate_and_convert_image(img_path: str, target_dir: str) -> Optional[str]:
    """
    驗證並轉換圖片文件，確保其可用於Word文檔
    """
    try:
        if not os.path.exists(img_path):
            return None
            
        file_size = os.path.getsize(img_path)
        if file_size == 0:
            return None
            
        os.makedirs(target_dir, exist_ok=True)
        
        filename = os.path.basename(img_path)
        output_path = os.path.join(target_dir, f"converted_{filename}")
        
        with Image.open(img_path) as img:
            if img.mode in ['RGBA', 'LA']:
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'RGBA':
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img, mask=img.split()[1])
                img = background
            elif img.mode in ['P', 'CMYK', '1', 'L', 'I', 'F']:
                img = img.convert('RGB')
                
            try:
                img.save(output_path, 'JPEG', quality=95, optimize=True)
                return output_path
            except Exception as e:
                try:
                    output_path = output_path.rsplit('.', 1)[0] + '.png'
                    img.save(output_path, 'PNG', optimize=True)
                    return output_path
                except Exception as e2:
                    return None
                            
    except Image.UnidentifiedImageError:
        return None
    except Exception as e:
        logger.error(f"圖片處理過程中發生錯誤: {str(e)}")
        return None 