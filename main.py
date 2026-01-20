from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import FileResponse  # 將文件作爲HTTP請求返回給客戶端(即Word文檔返回)
from typing import List, Optional
import os  
import json  
import logging
from docx.shared import Mm  # 設置Word文檔的相關尺寸
from datetime import datetime
import time
import uvicorn
from generate_word_report import createReport

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()  # 創建FastAPI應用實例


# 添加健康檢查端點
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/foodrecall_report")
async def foodrecall_report(request: Request):
    data = await request.json()
    user_id = data.get("userId","admin")
    # 刪除歷史文件
    files = os.listdir(r"./data")
    filelink = ['./data/'+f for f in files if f.endswith(('.docx'))]  # 得到data目錄下的所有.docx文件的路徑
    timestamp = time.time() - 3600
    n = len(filelink)
    if n > 0:
        for i in range(len(filelink)):
            if os.path.getctime(filelink[i]) < timestamp:
                os.remove(filelink[i])  # datatime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d-%H-%M-%S-%f') 
            else:
                pass
    else:
        pass

    try:
        filename = await createReport(data, userId=user_id)
        return filename
    except Exception as e:
        logger.error(f"生成報告時發生錯誤: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 下載文件
@app.get("/download_file/{filename}")
async def download_file(filename: str):
    return FileResponse(r"./data/{}".format(filename), media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document', filename=filename) 
 
if __name__ == "__main__":
    uvicorn.run(
        app="main:app",  # main:app 中的 main 必須與腳本文件名相同
        host="0.0.0.0",
        port=8000,
        reload=True)