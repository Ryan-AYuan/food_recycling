import requests
import fitz  # PyMuPDF
import os
import shutil
from PIL import Image, ImageOps
import cv2
import numpy as np
from urllib.parse import urlparse
import json
from typing import List, Dict, Any
import time
from tqdm import tqdm
import ssl
import urllib3
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PDFImageExtractor:
    def __init__(self, output_dir="extracted_images", proxy=None):
        self.output_dir = output_dir
        self.proxy = proxy
        os.makedirs(output_dir, exist_ok=True)
    
    def download_pdf(self, url: str, output_dir: str = None) -> str:
        """下載PDF文件，支持HTTP/HTTPS URL和本地文件路徑(file://協議)"""
        
        if output_dir is None:
            output_dir = self.output_dir
        
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 檢查是否為本地文件URL (file://協議)
        if url.startswith('file://'):
            return self._handle_local_file(url, output_dir)
        
        # 處理HTTP/HTTPS URL
        return self._download_remote_pdf(url, output_dir)
    
    def _handle_local_file(self, file_url: str, output_dir: str) -> str:
        """處理本地文件URL"""
        
        # 從file://URL中提取本地路徑
        local_path = file_url.replace('file:///', '').replace('/', os.sep)
        
        # 在Windows上處理路徑
        if os.name == 'nt' and local_path.startswith(os.sep):
            local_path = local_path[1:]
        
        # 檢查文件是否存在
        if not os.path.exists(local_path):
            logger.warning(f"本地PDF文件不存在: {local_path}")
            return None
        
        # 檢查是否為PDF文件
        if not local_path.lower().endswith('.pdf'):
            logger.warning(f"文件不是PDF格式: {local_path}")
            return None
        
        # 獲取文件信息
        filename = os.path.basename(local_path)
        
        # 如果文件已經在輸出目錄中，直接返回
        target_path = os.path.join(output_dir, filename)
        if os.path.abspath(local_path) == os.path.abspath(target_path):
            return target_path
        
        # 複製文件到輸出目錄
        try:
            shutil.copy2(local_path, target_path)
            return target_path
        except Exception as e:
            logger.error(f"複製文件失敗: {e}")
            return local_path
    
    def _download_remote_pdf(self, url: str, output_dir: str) -> str:
        """下載遠程PDF文件，支持代理、超時、重試和進度顯示"""
        
        # 禁用SSL警告
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        max_retries = 3
        timeout = 2
        
        for attempt in range(max_retries):
            try:
                logger.info(f"嘗試下載PDF文件 (第{attempt + 1}次): {url}")
                
                # 設置請求頭部
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,application/octet-stream,*/*',
                    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
                
                # 設置代理
                proxies = None
                if self.proxy:
                    proxies = {
                        'http': self.proxy,
                        'https': self.proxy
                    }
                
                # 創建session以保持連接
                session = requests.Session()
                session.headers.update(headers)
                session.verify = False  # 如果遇到SSL問題，可以設置為False
                
                # 首先發送HEAD請求獲取文件大小
                try:
                    head_response = session.head(url, proxies=proxies, timeout=timeout)
                    total_size = int(head_response.headers.get('content-length', 0))
                except:
                    total_size = 0
                
                # 發送GET請求下載文件
                response = session.get(url, stream=True, proxies=proxies, timeout=timeout)
                response.raise_for_status()
                
                filename = None
                
                # 從URL或響應頭提取文件名
                if 'content-disposition' in response.headers:
                    import re
                    cd = response.headers['content-disposition']
                    filename_match = re.findall('filename="(.+)"', cd)
                    if filename_match:
                        filename = filename_match[0]
                
                if not filename:
                    parsed_url = urlparse(url)
                    filename = os.path.basename(parsed_url.path)
                    if not filename.endswith('.pdf'):
                        filename = f"document_{hash(url) % 10000}.pdf"
                
                filepath = os.path.join(output_dir, filename)
                
                # 下載文件
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # 驗證下載的文件
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.info(f"PDF下載完成: {filepath}")
                    return filepath
                else:
                    raise Exception("下載的文件為空或不存在")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"下載超時 (第{attempt + 1}次嘗試)")
                if attempt < max_retries - 1:
                    time.sleep(attempt + 1)
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"連接錯誤 (第{attempt + 1}次嘗試): {e}")
                if attempt < max_retries - 1:
                    time.sleep(attempt + 1)
                    
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP錯誤 (第{attempt + 1}次嘗試): {e}")
                if e.response.status_code in [429, 502, 503, 504]:  # 可重試的錯誤
                    if attempt < max_retries - 1:
                        time.sleep(attempt + 1)
                else:
                    break  # 不可重試的錯誤，直接退出
                    
            except Exception as e:
                logger.warning(f"下載失敗 (第{attempt + 1}次嘗試): {e}")
                if attempt < max_retries - 1:
                    time.sleep(attempt + 1)
        
        logger.error(f"所有重試都失敗了，無法下載PDF文件: {url}")
        return None
    
    def convert_pdf_to_images(self, pdf_path: str) -> List[str]:
        """將PDF的每一頁轉換為PNG格式的圖片，使用倍數縮放以提供更高質量的圖片輸出

        Args:
            pdf_path (str): PDF文件路徑

        Returns:
            List[str]: 生成的圖片文件路徑列表
        """
        converted_images = []
        
        if not os.path.exists(pdf_path):
            logger.warning(f"PDF文件不存在: {pdf_path}")
            return []
        
        try:
            doc = fitz.open(pdf_path)
            pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
            total_pages = len(doc)
            
            logger.info(f"開始轉換PDF: {os.path.basename(pdf_path)}，總頁數: {total_pages}")
            
            for page_num in range(total_pages):
                try:
                    page = doc.load_page(page_num)
                    # 使用3倍縮放以獲得更大更清晰的圖片
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                    
                    # 生成圖片文件名
                    img_filename = f"{pdf_name}_{page_num + 1}.png"
                    img_path = os.path.join(self.output_dir, img_filename)
                    
                    # 保存圖片
                    pix.save(img_path)
                    converted_images.append(img_path)
                    
                except Exception as page_error:
                    logger.error(f"處理第{page_num+1}頁時出錯: {page_error}")
                    continue
            
            doc.close()
            
            if converted_images:
                logger.info(f"PDF轉換完成，共生成 {len(converted_images)} 張圖片")
            else:
                logger.warning("PDF轉換失敗，未生成任何圖片")
            
            return converted_images
            
        except Exception as e:
            logger.error(f"PDF轉換失敗 {pdf_path}: {e}")
            return []
    
    def extract_images_from_pdf(self, pdf_path: str) -> List[str]:
        """從PDF中提取所有圖片，支持進度顯示"""
        extracted_images = []
        
        if not os.path.exists(pdf_path):
            # logger.warning(f" PDF文件不存在: {pdf_path}")
            return []
        
        try:
            # logger.info(f"\n 開始提取PDF圖片: {os.path.basename(pdf_path)}")
            doc = fitz.open(pdf_path)
            pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
            total_pages = len(doc)
            
            # print(f"PDF總頁數: {total_pages}")
            
            # 統計總圖片數量
            total_images = 0
            for page_num in range(total_pages):
                page = doc.load_page(page_num)
                image_list = page.get_images(full=True)
                total_images += len(image_list)
            
            # print(f"發現圖片總數: {total_images}")
            
            if total_images == 0:
                # print(" 此PDF中沒有發現圖片")
                doc.close()
                return []
            
            # 重新遍歷並提取圖片 - 不采用進度條
            extracted_count = 0
            for page_num in range(total_pages):
                try:
                    page = doc.load_page(page_num)
                    image_list = page.get_images(full=True)
                    
                    for img_index, img in enumerate(image_list):
                        try:
                            xref = img[0]
                            pix = fitz.Pixmap(doc, xref)
                            
                            # 檢查圖片格式和大小
                            if pix.n - pix.alpha < 4:  # 確保是RGB或灰度圖像
                                # 檢查圖片大小，過濫小圖片
                                if pix.width >= 50 and pix.height >= 50:
                                    img_filename = f"{pdf_name}_page{page_num+1}_img{img_index+1}.png"
                                    img_path = os.path.join(self.output_dir, img_filename)
                                    
                                    if pix.alpha:
                                        pix = fitz.Pixmap(fitz.csRGB, pix)
                                    
                                    pix.save(img_path)
                                    extracted_images.append(img_path)
                                    extracted_count += 1
                                else:
                                    # 跳過太小的圖片
                                    pass
                            
                            if pix:
                                pix = None
                                
                        except Exception as img_error:
                            logger.error(f"\n 提取第{page_num+1}頁第{img_index+1}張圖片時出錯: {img_error}")
                        
                except Exception as page_error:
                    logger.error(f"\n 處理第{page_num+1}頁時出錯: {page_error}")
                    continue

            # 進度條顯示            
            # with tqdm(total=total_images, desc="提取圖片", unit="張") as pbar:
            #     for page_num in range(total_pages):
            #         try:
            #             page = doc.load_page(page_num)
            #             image_list = page.get_images(full=True)
                        
            #             for img_index, img in enumerate(image_list):
            #                 try:
            #                     xref = img[0]
            #                     pix = fitz.Pixmap(doc, xref)
                                
            #                     # 檢查圖片格式和大小
            #                     if pix.n - pix.alpha < 4:  # 確保是RGB或灰度圖像
            #                         # 檢查圖片大小，過濫小圖片
            #                         if pix.width >= 50 and pix.height >= 50:
            #                             img_filename = f"{pdf_name}_page{page_num+1}_img{img_index+1}.png"
            #                             img_path = os.path.join(self.output_dir, img_filename)
                                        
            #                             if pix.alpha:
            #                                 pix = fitz.Pixmap(fitz.csRGB, pix)
                                        
            #                             pix.save(img_path)
            #                             extracted_images.append(img_path)
            #                             extracted_count += 1
                                        
            #                             # 獲取圖片信息
            #                             file_size = os.path.getsize(img_path) / 1024  # KB
            #                             pbar.set_postfix({
            #                                 '當前': f"{pix.width}x{pix.height}",
            #                                 '大小': f"{file_size:.1f}KB"
            #                             })
            #                         else:
            #                             pbar.set_postfix({'跳過': f"太小({pix.width}x{pix.height})"})
                                
            #                     if pix:
            #                         pix = None
                                    
            #                 except Exception as img_error:
            #                     print(f"\n 提取第{page_num+1}頁第{img_index+1}張圖片時出錯: {img_error}")
                            
            #                 pbar.update(1)
                            
            #         except Exception as page_error:
            #             print(f"\n 處理第{page_num+1}頁時出錯: {page_error}")
            #             continue

            doc.close()
            
            if extracted_count > 0:
                # logger.info(f"\n 成功提取 {extracted_count} 張圖片")
                pass
            else:
                # logger.info(f"\n 沒有提取到有效圖片（可能圖片太小或格式不支持）")
                pass
                
            return extracted_images
            
        except Exception as e:
            # logger.error(f"\n 提取圖片失敗 {pdf_path}: {e}")
            return []
    
    def auto_crop_image(self, image_path: str) -> str:
        """自動裁剪圖片，去除白邊，支持多種裁剪策略"""
        
        if not os.path.exists(image_path):
            # logger.error(f"\n 圖片文件不存在: {image_path}")
            return image_path
        
        try:
            filename = os.path.basename(image_path)
            # print(f" 裁剪圖片: {filename}")
            
            # 使用PIL打開圖片
            with Image.open(image_path) as img:
                original_size = img.size
                # print(f"   原始尺寸: {original_size[0]}x{original_size[1]}")
                
                # 轉換為RGB模式
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # 轉換為numpy數組
                img_array = np.array(img)
                
                # 轉換為灰度圖像用於邊緣檢測
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
                
                # 嘗試多種裁剪策略
                cropped_img = None
                crop_method = "未知"
                
                # 策略1: 基於輪廓檢測的裁剪
                try:
                    # 使用閾值處理
                    _, thresh = cv2.threshold(gray, 240, 255, cv2.THRESH_BINARY_INV)
                    
                    # 查找輪廓
                    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    
                    if contours:
                        # 找到最大的輪廓
                        largest_contour = max(contours, key=cv2.contourArea)
                        contour_area = cv2.contourArea(largest_contour)
                        total_area = img.width * img.height
                        
                        # 只有當輪廓面積合理時才使用
                        if contour_area > total_area * 0.1:  # 至少佔10%的面積
                            x, y, w, h = cv2.boundingRect(largest_contour)
                            
                            # 添加適當的邊距
                            margin = min(20, min(img.width, img.height) // 20)
                            x = max(0, x - margin)
                            y = max(0, y - margin)
                            w = min(img.width - x, w + 2 * margin)
                            h = min(img.height - y, h + 2 * margin)
                            
                            # 確保裁剪區域有效
                            if w > 50 and h > 50:
                                cropped_img = img.crop((x, y, x + w, y + h))
                                crop_method = "輪廓檢測"
                except Exception as contour_error:
                    # logger.info(f"\n 輪廓檢測失敗: {contour_error}")
                    pass
                
                # 策略2: 基於邊緣檢測的裁剪
                if cropped_img is None:
                    try:
                        # 使用Canny邊緣檢測
                        edges = cv2.Canny(gray, 50, 150)
                        
                        # 找到非零像素的邊界
                        coords = np.column_stack(np.where(edges > 0))
                        if len(coords) > 0:
                            y_min, x_min = coords.min(axis=0)
                            y_max, x_max = coords.max(axis=0)
                            
                            # 添加邊距
                            margin = 15
                            x_min = max(0, x_min - margin)
                            y_min = max(0, y_min - margin)
                            x_max = min(img.width, x_max + margin)
                            y_max = min(img.height, y_max + margin)
                            
                            if (x_max - x_min) > 50 and (y_max - y_min) > 50:
                                cropped_img = img.crop((x_min, y_min, x_max, y_max))
                                crop_method = "邊緣檢測"
                    except Exception as edge_error:
                        # logger.error(f"\n 邊緣檢測失敗: {edge_error}")
                        pass
                
                # 策略3: PIL自動裁剪白邊
                if cropped_img is None:
                    try:
                        # 嘗試不同的border值
                        for border in [10, 20, 30]:
                            try:
                                temp_cropped = ImageOps.crop(img, border=border)
                                if temp_cropped.size[0] > 50 and temp_cropped.size[1] > 50:
                                    cropped_img = temp_cropped
                                    crop_method = f"PIL自動裁剪(border={border})"
                                    break
                            except:
                                continue
                    except Exception as pil_error:
                        # logger.info(f"\n PIL自動裁剪失敗: {pil_error}")
                        pass
                
                # 如果所有策略都失敗，返回原圖
                if cropped_img is None:
                    # logger.info(f"所有裁剪策略都失敗，保持原圖")
                    return image_path
                
                # 檢查裁剪效果
                new_size = cropped_img.size
                size_reduction = (1 - (new_size[0] * new_size[1]) / (original_size[0] * original_size[1])) * 100
                
                # print(f"   裁剪方法: {crop_method}")
                # print(f"   新尺寸: {new_size[0]}x{new_size[1]}")
                # print(f"   面積減少: {size_reduction:.1f}%")
                
                # 生成裁剪後的文件名
                base_name = os.path.splitext(image_path)[0]
                cropped_path = f"{base_name}_cropped.png"
                
                # 保存裁剪後的圖片
                cropped_img.save(cropped_path, 'PNG', optimize=True)
                
                # 檢查文件大小
                original_file_size = os.path.getsize(image_path) / 1024
                cropped_file_size = os.path.getsize(cropped_path) / 1024
                
                # print(f"    裁剪完成: {os.path.basename(cropped_path)}")
                # print(f"   文件大小: {original_file_size:.1f}KB → {cropped_file_size:.1f}KB")
                
                return cropped_path
                
        except Exception as e:
            logger.error(f"\n 裁剪圖片失敗 {filename}: {e}")
            return image_path
    
    def process_pdf_urls(self, pdf_urls: List[str], output_dir: str = None) -> Dict[str, Any]:
        """處理多個PDF(支持HTTP/HTTPS URL和本地文件路徑(file://協議))，將每頁轉換為PNG圖片"""
        if output_dir is None:
            output_dir = self.output_dir
        
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
        results = {
            'success': [],
            'failed': [],
            'image_links': [],
            'total_images': 0,
            'processing_time': 0,
            'statistics': {
                'total_pdfs': len(pdf_urls),
                'successful_pdfs': 0,
                'failed_pdfs': 0,
                'total_download_size': 0,
                'total_images_converted': 0
            }
        }
        
        start_time = time.time()
        
        for i, url in enumerate(pdf_urls, 1):
            try:
                # 下載PDF
                pdf_path = self.download_pdf(url, output_dir)
                if not pdf_path:
                    error_msg = 'PDF下載失敗'
                    results['failed'].append({'url': url, 'error': error_msg})
                    results['statistics']['failed_pdfs'] += 1
                    continue
                
                # 記錄下載文件大小
                if os.path.exists(pdf_path):
                    file_size = os.path.getsize(pdf_path)
                    results['statistics']['total_download_size'] += file_size
                
                # 轉換PDF頁面為圖片
                image_paths = self.convert_pdf_to_images(pdf_path)
                if not image_paths:
                    error_msg = 'PDF轉換失敗'
                    results['failed'].append({'url': url, 'error': error_msg})
                    results['statistics']['failed_pdfs'] += 1
                    continue
                
                # 更新統計信息
                results['success'].append({
                    'url': url,
                    'pdf_path': pdf_path,
                    'image_paths': image_paths,
                    'image_count': len(image_paths)
                })
                results['image_links'].extend(image_paths)
                results['total_images'] += len(image_paths)
                results['statistics']['successful_pdfs'] += 1
                results['statistics']['total_images_converted'] += len(image_paths)
                
            except Exception as e:
                error_msg = str(e)
                results['failed'].append({'url': url, 'error': error_msg})
                results['statistics']['failed_pdfs'] += 1
        
        # 計算處理時間
        results['processing_time'] = time.time() - start_time
        
        return results

def main():
    """主函數，演示如何使用PDFImageExtractor處理單個PDF文件"""
    # 示例PDF文件路徑
    pdf_path = "data/pdf_files_from_fsis/bmV3c0A2NzE5OTYzNTRAMjAyNS0wNy0wNlQxNTowMDowMC4wMDBa.pdf"
    
    # 創建提取器實例
    extractor = PDFImageExtractor("data/pdf_files_from_fsisAndfsa_toImages")
    
    logger.info("PDF轉換工具")
    logger.info(f"處理PDF文件: {pdf_path}")
    
    try:
        # 將文件路徑轉換為file://URL格式
        pdf_url = f"file:///{os.path.abspath(pdf_path).replace(os.sep, '/')}"
        
        # 處理PDF文件
        results = extractor.process_pdf_urls([pdf_url])
        
        # 顯示處理結果
        if results['success']:
            logger.info("\n處理完成！")
            pdf_result = results['success'][0]
            logger.info(f"成功轉換圖片: {pdf_result['image_count']} 張")
            logger.info(f"處理時間: {results['processing_time']:.2f} 秒")
            logger.info(f"結果保存在: {extractor.output_dir}")
            
    except Exception as e:
        logger.error(f"處理失敗: {e}")

if __name__ == "__main__":
    main()
