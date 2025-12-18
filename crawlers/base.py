"""
爬蟲基礎類別
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import AsyncGenerator, Dict, Callable, Optional
import httpx


class BaseCrawler(ABC):
    """爬蟲基礎抽象類別"""

    def __init__(self, city_name: str, start_date: date, end_date: date):
        self.city_name = city_name
        self.start_date = start_date
        self.end_date = end_date
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """進入異步上下文管理器"""
        # verify=False 跳過 SSL 憑證驗證（政府網站憑證鏈常不完整）
        self.client = httpx.AsyncClient(timeout=60.0, follow_redirects=True, verify=False)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """離開異步上下文管理器"""
        if self.client:
            await self.client.aclose()

    @abstractmethod
    async def fetch_data(
        self,
        on_progress: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[Dict, None]:
        """
        異步生成器，逐筆返回爬取的資料

        Args:
            on_progress: 進度回調函式，接收日誌訊息

        Yields:
            Dict: 包含 'date', 'index_key', 'data' 的字典
        """
        pass

    def log(self, message: str, on_progress: Optional[Callable[[str], None]] = None):
        """記錄日誌"""
        if on_progress:
            on_progress(message)
