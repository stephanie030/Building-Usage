"""
MCGBM 系統爬蟲
支援城市：基隆市、新北市、桃園市、新竹市、台中市
"""
import json
from datetime import date
from typing import AsyncGenerator, Dict, Callable, Optional
import httpx

from .base import BaseCrawler
from utils.data_processor import (
    generate_url_params,
    tw_str_to_date,
    refine_mcbgm
)


class MCGBMCrawler(BaseCrawler):
    """MCGBM 系統爬蟲"""

    def __init__(
        self,
        city_name: str,
        base_url: str,
        start_date: date,
        end_date: date
    ):
        super().__init__(city_name, start_date, end_date)
        self.base_url = base_url

    async def fetch_data(
        self,
        on_progress: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[Dict, None]:
        """爬取 MCGBM 系統資料"""

        # 轉換為民國年份
        start_year = self.start_date.year - 1911
        end_year = self.end_date.year - 1911

        total_count = 0

        for year in range(start_year, end_year + 1):
            for license_type in ['建造執照', '使用執照']:
                self.log(f"正在爬取 {self.city_name} {year + 1911}年 {license_type}...", on_progress)

                for start in range(1, 5000, 100):
                    url = self.base_url + generate_url_params(license_type, start, year)

                    try:
                        response = await self.client.get(url)
                        response.raise_for_status()

                        # 移除特殊字元並解析 JSON
                        text = response.text.replace("\x05", "")
                        data = json.loads(text)

                        rows = data.get('data', [])
                        if not rows:
                            break  # 沒有更多資料

                        for row in rows:
                            try:
                                refined_data = refine_mcbgm(row)
                                item_date = tw_str_to_date(refined_data["發照日期"])

                                # 檢查日期是否在範圍內
                                if item_date and self.start_date <= item_date <= self.end_date:
                                    total_count += 1
                                    yield {
                                        'date': item_date,
                                        'index_key': refined_data["_id"],
                                        'data': refined_data
                                    }
                            except Exception as e:
                                self.log(f"處理資料時發生錯誤: {e}", on_progress)
                                continue

                        if len(rows) < 100:
                            break  # 已經是最後一批

                    except httpx.HTTPError as e:
                        self.log(f"HTTP 請求錯誤: {e}", on_progress)
                        break
                    except json.JSONDecodeError as e:
                        self.log(f"JSON 解析錯誤: {e}", on_progress)
                        break

        self.log(f"{self.city_name} 爬取完成，共 {total_count} 筆資料", on_progress)
