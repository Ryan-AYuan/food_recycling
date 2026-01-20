# 食品安全回收報告生成系統 (Food Safety Recall Report Generator)

這是一個基於 **FastAPI** 和 **Python** 構建的後端自動化系統，旨在從全球多個食品安全監管機構（如美國 FDA、日本消費者廳、香港食安中心、紐西蘭 MPI 等）獲取食品召回數據，並利用 **LLM (Dify)** 進行數據增強（翻譯、OCR、非結構化信息提取），最終自動生成標準化的 **Word (.docx)** 報告。

---

## 📂 項目結構與核心腳本詳解

### 1. 核心服務入口：`main.py`
這是項目的 Web 服務器入口，負責接收外部請求並協調報告生成流程。

*   **主要功能**：
    *   **啟動服務**：使用 `uvicorn` 啟動 FastAPI 應用（默認端口 8000）。
    *   **API 端點**：
        *   `POST /foodrecall_report`: 接收包含 `globalIds`（帖子ID列表）的 JSON 請求，觸發報告生成。
        *   `GET /download_file/{filename}`: 提供生成好的 Word 文檔下載路徑。
        *   `GET /health`: 健康檢查接口。
    *   **自動清理**：每次請求時，會檢查 `./data` 目錄，自動刪除創建時間超過 1 小時的 `.docx` 舊文件，防止磁盤堆積。

### 2. 業務邏輯核心：`generate_word_report.py`
這是項目的**大腦**，包含了最複雜的業務邏輯、數據清洗規則和流程控制。

*   **主要流程 (`createReport` 函數)**：
    1.  **環境清理**：調用 `clean_old_files` 清理臨時圖片和 PDF。
    2.  **數據獲取與組裝 (`create_json`)**：
        *   調用 `data_utils.getData` 獲取原始數據。
        *   **PDF 處理**：針對 FSIS/FSA 來源，自動識別 PDF 鏈接並下載，提取其中的圖片（因為這些機構常把關鍵信息放在 PDF 圖片中）。
        *   **正則提取 (Regex)**：針對不同來源（FDA, MPI, FSANZ, Canada, 日本消費者廳等）編寫了數十個特定的正則表達式，從 HTML 文本中提取「分銷信息 (Distribution)」和「回收原因 (Recycling Reason)」。
        *   **日本數據特殊處理**：針對日本消費者廳數據，有多層次的關鍵詞匹配邏輯（如「販売地域」、「回収理由」等）。
    3.  **PDF 下載與 OCR**：
        *   並發下載 CDPH (加州) 和 HK (香港) 的 PDF 文件。
        *   調用 Dify 工作流 (`PDF2Content`) 對 PDF 內容進行識別和提取。
    4.  **Dify 工作流調用**：
        *   執行 `foodsafety` 工作流：對數據進行進一步的結構化清洗和判斷。
        *   執行 `translate` 工作流：對香港（非 PDF 來源）和日本的回收原因進行翻譯。
    5.  **數據合併**：將正則提取的數據、Dify 返回的數據、原始數據進行優先級合併。
    6.  **文檔渲染**：
        *   處理圖片：自動下載、驗證、轉換（WebP/RGBA 轉 JPG/PNG）、自適應縮放。
        *   使用 `docxtpl` 將最終數據填入 `report_template.docx`。

### 3. 數據處理工具：`data_utils.py`
提供通用的數據操作函數。

*   **關鍵函數**：
    *   `getData(globalIds)`: 訪問 `ersinfotech.com` 接口獲取原始 JSON 數據。
    *   `html_to_markdown(html_content)`: 將複雜的 HTML 轉換為 Markdown，便於 LLM 理解和正則匹配。包含特殊的 PDF 鏈接保留邏輯。
    *   `transform_mydict_to_mydict_list_final(...)`: **關鍵函數**。負責將扁平的字典轉換為列表結構，並處理「來源名稱標準化」（如將 "Ministry for Primary Industries" 統一為 "NZ MPI"）。
    *   `clean_old_files(...)`: 根據文件後綴和時間戳清理舊文件。

### 4. API 交互工具：`api_utils.py`
封裝了與 **Dify (LLM 平台)** 的所有異步交互。

*   **特點**：
    *   **全異步 (Async/Await)**：使用 `aiohttp` 進行高並發請求，提高處理速度。
    *   **功能封裝**：
        *   `upload_files_async`: 併發上傳文件到 Dify 知識庫/輸入端。
        *   `_run_workflow_async`: 執行 Dify Workflow，包含重試機制 (Max Retries = 3)。
    *   **特定工作流封裝**：包含針對 PDF 解析、圖片解析等多種場景的封裝函數。

### 5. PDF 處理工具：`pdf_utils.py` & `pdf_image_extractor.py`
處理 PDF 下載、轉換和信息提取。

*   **`pdf_utils.py`**：
    *   **抗指紋下載**：`download_pdf_for_fsis_and_fsa` 使用 `curl_cffi` 模擬真實瀏覽器 (Chrome 120) 的 TLS 指紋，專門用於繞過 FSIS 等網站的反爬蟲攔截。
    *   **普通下載**：`download_pdf` 使用標準的 `aiohttp`。
    *   **流程控制**：`process_pdf_with_extractor` 協調下載 -> 轉換 -> 重命名的全過程。

*   **`pdf_image_extractor.py`**：
    *   **底層轉換**：使用 `PyMuPDF (fitz)` 將 PDF 頁面渲染為高分辨率圖片。
    *   **智能裁切 (`auto_crop_image`)**：使用 **OpenCV** 算法（輪廓檢測、Canny 邊緣檢測）自動去除圖片的大面積白邊，優化 Word 報告的排版效果。

### 6. 圖片處理工具：`image_utils.py`
負責產品圖片的下載和格式化。

*   **特點**：
    *   **反爬蟲策略**：內置隨機 `User-Agent` 池和動態 `Referer` 設置，防止被目標網站封鎖。
    *   **格式轉換**：`validate_and_convert_image` 自動處理 RGBA (透明背景)、CMYK 模式圖片，統一轉換為 RGB 模式的 JPEG/PNG，確保 Word 文檔兼容性。
    *   **併發控制**：使用 `asyncio.Semaphore` 限制最大並發數，避免對目標服務器造成過大壓力。

---

## 🛠️ 環境配置與依賴

### Python 版本
建議使用 **Python 3.10+** (代碼中大量使用了異步特性)。

### 關鍵依賴庫
*   **Web 框架**: `fastapi`, `uvicorn`
*   **網絡請求**: `httpx`, `requests`, `aiohttp`, `curl_cffi` (關鍵：用於繞過 TLS 指紋)
*   **文檔處理**: `python-docx`, `docxtpl`
*   **PDF 與圖片**: `PyMuPDF (fitz)`, `Pillow (PIL)`, `opencv-python-headless` (cv2)
*   **其他**: `beautifulsoup4`, `python-dotenv`

### 環境變量 (.env)
項目根目錄需配置 `.env` 文件，包含以下關鍵變量：
```ini
API_WORKFLOW_RUN_URL_PRO=...  # Dify 工作流 API 地址
API_FILE_UPLOAD_URL_PRO=...   # Dify 文件上傳地址
API_KEY_PRO_V2=...            # FoodSafety 工作流 Key
API_KEY_PRO_PDF2CONTENT=...   # PDF 解析工作流 Key
```

---

## 📝 後期維護指南

### 1. 添加新的數據來源
如果需要支持新的國家或機構：
1.  **修改 `data_utils.py`**：在 `source_mapping` 和 `source_patterns` 字典中添加新機構的名稱映射。
2.  **修改 `generate_word_report.py`**：
    *   在 `create_json` 函數中，觀察新來源的 HTML 結構，編寫相應的 Regex 提取邏輯（參考 `rasff_pattern` 或 `fda_pattern`）。
    *   在 `createReport` 的 Title 生成邏輯中，定義新來源的標題格式（是否需要拼接回收原因等）。

### 2. 正則表達式維護
**這是最容易出問題的地方。** 目標網站（如 FDA, MPI）一旦改版，`generate_word_report.py` 中的 Regex 就會失效。
*   **維護建議**：
    *   當報告中出現內容為 `--` 或提取不完整時，首先檢查對應來源的 HTML 源碼是否變更。
    *   建議將 Regex 提取邏輯逐步遷移到 Dify (LLM) 進行處理，以提高抗干擾能力。

### 3. Dify 工作流變更
如果 Dify 平台上的工作流邏輯（輸入/輸出變量名）發生變化：
*   需要同步修改 `api_utils.py` 中的 Payload 結構。
*   需要同步修改 `generate_word_report.py` 中解析 `outputs` 的鍵名。

### 4. 依賴庫升級
*   **`curl_cffi`**：該庫用於模擬瀏覽器指紋，如果目標網站升級了反爬策略，可能需要升級此庫或更新 `impersonate` 參數（目前為 `chrome120`）。

### 5. 常見報錯處理
*   **圖片下載失敗 (403/404)**：通常是反爬蟲觸發。檢查 `image_utils.py` 中的 `User-Agent` 列表是否過舊，或嘗試增加 `download_delay`。
*   **Word 生成報錯**：通常是圖片尺寸或格式問題。檢查 `validate_and_convert_image` 是否能正確處理特殊格式（如 WebP, AVIF）。
