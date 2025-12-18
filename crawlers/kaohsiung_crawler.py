"""
高雄市爬蟲
使用純 JSON API
"""
from datetime import date, timedelta
from typing import AsyncGenerator, Dict, Callable, Optional
import httpx

from .base import BaseCrawler
from utils.data_processor import (
    generate_dates,
    tw_year_format,
    tw_str_to_date,
    refine_kaohsiung_json
)


class KaohsiungCrawler(BaseCrawler):
    """高雄市爬蟲"""

    def __init__(self, city_name: str, start_date: date, end_date: date):
        super().__init__(city_name, start_date, end_date)
        self.base_url = "https://buildmis.kcg.gov.tw/bupic/pages/jsapi/querylic"
        self.detail_url = "https://buildmis.kcg.gov.tw/bupic/pages/jsapi/getLicenseInfo"

    def _get_headers(self, cookies: str = "") -> Dict:
        """取得請求標頭"""
        return {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookies,
            'Host': 'buildmis.kcg.gov.tw',
            'Origin': 'https://buildmis.kcg.gov.tw',
            'Referer': 'https://buildmis.kcg.gov.tw/bupic/pages/querylic',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

    async def fetch_data(
        self,
        on_progress: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[Dict, None]:
        """爬取高雄市資料"""

        # 先訪問首頁取得 cookies
        self.log(f"正在初始化 {self.city_name} 連線...", on_progress)

        try:
            init_response = await self.client.get("https://buildmis.kcg.gov.tw/bupic/pages/querylic")
            cookies = "; ".join([f"{k}={v}" for k, v in self.client.cookies.items()])
        except httpx.HTTPError as e:
            self.log(f"初始化連線失敗: {e}", on_progress)
            return

        headers = self._get_headers(cookies)
        total_count = 0

        # 每 3 天為一批
        delta = timedelta(days=3)
        for from_date in generate_dates(self.start_date, self.end_date, delta=delta):
            to_date = min(from_date + delta - timedelta(days=1), self.end_date)

            self.log(f"正在爬取 {from_date} ~ {to_date}...", on_progress)

            payload = {
                'qrytyp': '5',
                'lic_yy': '',
                'lic_kind': '',
                'lic_no1': '',
                'p01_name': '',
                'addradr': '',
                'date_s': tw_year_format(from_date),
                'date_e': tw_year_format(to_date),
                'dist': '',
                'section': '',
                'road_no1': '',
                'road_no2': '',
                'yy': '',
                'mon': '',
                'dd': '',
                'reg_yy': '',
                'reg_no': '',
                'reg_nochkcod': '',
            }

            try:
                response = await self.client.post(
                    self.base_url,
                    headers=headers,
                    data=payload
                )
                response.raise_for_status()
                data = response.json()

                for row in data:
                    index_key = row.get('dkey', '')
                    item_date = tw_str_to_date(row.get('licdate', ''))

                    if not index_key or not item_date:
                        continue

                    # 取得詳細資料
                    try:
                        detail_response = await self.client.post(
                            self.detail_url,
                            headers=headers,
                            data={'key': index_key}
                        )
                        detail_response.raise_for_status()
                        detail_data = detail_response.json()

                        refined_data = refine_kaohsiung_json(detail_data, index_key)
                        if refined_data:
                            total_count += 1
                            yield {
                                'date': item_date,
                                'index_key': index_key,
                                'data': refined_data
                            }

                    except Exception as e:
                        self.log(f"取得詳細資料失敗 ({index_key}): {e}", on_progress)
                        continue

            except httpx.HTTPError as e:
                self.log(f"HTTP 請求錯誤: {e}", on_progress)
                continue
            except Exception as e:
                self.log(f"處理資料時發生錯誤: {e}", on_progress)
                continue

        self.log(f"{self.city_name} 爬取完成，共 {total_count} 筆資料", on_progress)
