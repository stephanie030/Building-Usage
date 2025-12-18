"""
FastAPI 後端主程式
"""
import asyncio
import os
import uuid
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers import get_crawler, get_supported_cities, CITY_CONFIG
from utils.excel_writer import ExcelWriter


# ============== 資料模型 ==============

class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CrawlRequest(BaseModel):
    cities: List[str] = Field(..., min_length=1, description="城市列表")
    start_date: date = Field(..., description="開始日期")
    end_date: date = Field(..., description="結束日期")

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        start = info.data.get('start_date')
        if start and v < start:
            raise ValueError('結束日期必須大於等於開始日期')
        return v

    @field_validator('cities')
    @classmethod
    def validate_cities(cls, v):
        supported = get_supported_cities()
        for city in v:
            if city not in supported:
                raise ValueError(f"不支援的城市: {city}")
        return v


class TaskProgress(BaseModel):
    current_city: str = ""
    total_cities: int = 0
    completed_cities: int = 0
    total_records: int = 0
    current_date: Optional[str] = None


class TaskResponse(BaseModel):
    task_id: str
    status: TaskStatus
    cities: List[str]
    start_date: date
    end_date: date
    progress: TaskProgress
    created_at: datetime
    logs: List[str] = []
    files: Dict[str, Dict[str, str]] = {}
    error: Optional[str] = None


class CityInfo(BaseModel):
    id: str
    name: str
    system: str


# ============== 任務管理 ==============

# 全域任務儲存（簡單版使用記憶體）
tasks: Dict[str, Dict] = {}

# 輸出目錄
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


async def run_crawler_task(task_id: str):
    """執行爬蟲任務"""
    task = tasks.get(task_id)
    if not task:
        return

    task['status'] = TaskStatus.RUNNING
    task['progress']['total_cities'] = len(task['cities'])

    try:
        for i, city in enumerate(task['cities']):
            task['progress']['current_city'] = city
            task['progress']['completed_cities'] = i

            # 建立爬蟲
            crawler = get_crawler(city, task['start_date'], task['end_date'])

            # 建立 Excel 寫入器
            task_output_dir = os.path.join(OUTPUT_DIR, task_id)
            os.makedirs(task_output_dir, exist_ok=True)
            excel_writer = ExcelWriter(city, task_output_dir)

            def log_callback(msg: str):
                timestamp = datetime.now().strftime("%H:%M:%S")
                log_msg = f"[{timestamp}] {msg}"
                task['logs'].append(log_msg)
                # 只保留最新 100 條日誌
                if len(task['logs']) > 100:
                    task['logs'] = task['logs'][-100:]

            # 執行爬蟲
            async with crawler:
                async for item in crawler.fetch_data(on_progress=log_callback):
                    excel_writer.write_item(item)
                    task['progress']['total_records'] += 1

            # 儲存 Excel
            saved_files = excel_writer.save()
            task['files'][city] = saved_files

            log_callback(f"{city} 完成，已儲存 Excel 檔案")

        task['progress']['completed_cities'] = len(task['cities'])
        task['status'] = TaskStatus.COMPLETED

    except Exception as e:
        task['status'] = TaskStatus.FAILED
        task['error'] = str(e)
        task['logs'].append(f"[ERROR] {str(e)}")


# ============== FastAPI 應用程式 ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 啟動時
    print("建築執照爬蟲 API 啟動")
    yield
    # 關閉時
    print("建築執照爬蟲 API 關閉")


app = FastAPI(
    title="建築執照爬蟲 API",
    description="提供建築執照資料爬取服務",
    version="1.0.0",
    lifespan=lifespan
)

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== API 端點 ==============

@app.get("/")
async def root():
    """API 根端點"""
    return {"message": "建築執照爬蟲 API", "version": "1.0.0"}


@app.get("/api/v1/cities", response_model=List[CityInfo])
async def get_cities():
    """取得支援的城市列表"""
    cities = []
    for name, config in CITY_CONFIG.items():
        cities.append(CityInfo(
            id=name,
            name=name,
            system=config["system"]
        ))
    return cities


@app.post("/api/v1/crawl")
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """提交爬蟲任務"""
    task_id = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    task = {
        'task_id': task_id,
        'status': TaskStatus.QUEUED,
        'cities': request.cities,
        'start_date': request.start_date,
        'end_date': request.end_date,
        'created_at': datetime.now(),
        'progress': {
            'current_city': '',
            'total_cities': len(request.cities),
            'completed_cities': 0,
            'total_records': 0,
            'current_date': None
        },
        'logs': [],
        'files': {},
        'error': None
    }
    tasks[task_id] = task

    # 在背景執行爬蟲任務
    background_tasks.add_task(run_crawler_task, task_id)

    return {
        "task_id": task_id,
        "status": "queued",
        "message": "爬蟲任務已提交"
    }


@app.get("/api/v1/status/{task_id}")
async def get_task_status(task_id: str):
    """查詢任務狀態"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任務不存在")

    return TaskResponse(
        task_id=task['task_id'],
        status=task['status'],
        cities=task['cities'],
        start_date=task['start_date'],
        end_date=task['end_date'],
        progress=TaskProgress(**task['progress']),
        created_at=task['created_at'],
        logs=task['logs'][-20:],  # 只返回最新 20 條日誌
        files=task['files'],
        error=task['error']
    )


@app.get("/api/v1/logs/{task_id}")
async def get_task_logs(task_id: str):
    """取得任務日誌"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任務不存在")

    return {"logs": task['logs']}


@app.get("/api/v1/download/{task_id}")
async def download_file(task_id: str, city: str, license_type: str = "建造執照"):
    """下載結果檔案"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任務不存在")

    if city not in task['files']:
        raise HTTPException(status_code=404, detail="找不到該城市的檔案")

    file_path = task['files'][city].get(license_type)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="檔案不存在")

    filename = os.path.basename(file_path)
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.delete("/api/v1/tasks/{task_id}")
async def cancel_task(task_id: str):
    """取消任務"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任務不存在")

    if task['status'] in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
        task['status'] = TaskStatus.CANCELLED
        return {"message": "任務已取消"}
    else:
        return {"message": f"任務狀態為 {task['status']}，無法取消"}


@app.get("/api/v1/tasks")
async def list_tasks():
    """列出所有任務"""
    return [
        {
            'task_id': t['task_id'],
            'status': t['status'],
            'cities': t['cities'],
            'created_at': t['created_at']
        }
        for t in tasks.values()
    ]


# ============== 主程式入口 ==============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
