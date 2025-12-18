"""
新竹縣爬蟲
使用 JSONP API 和 HTML 解析
"""
import json
from datetime import date, datetime
from typing import AsyncGenerator, Dict, Callable, Optional
import httpx

from .base import BaseCrawler
from utils.data_processor import (
    generate_dates,
    tw_year_format,
    tw_str_to_date,
    hc_parse_html_bs4,
    refine_hsinchu_county
)


class HsinchuCountyCrawler(BaseCrawler):
    """新竹縣爬蟲"""

    def __init__(self, city_name: str, start_date: date, end_date: date):
        super().__init__(city_name, start_date, end_date)
        self.base_url = "https://build.hsinchu.gov.tw/bupic/pages/api/getLicdata?callback=ok"
        self.detail_url = "https://build.hsinchu.gov.tw/bupic/pages/queryInfoAction.do"

    def _get_headers(self, cookies: str = "") -> Dict:
        """取得請求標頭"""
        return {
            'Accept': 'text/javascript, application/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookies,
            'Host': 'build.hsinchu.gov.tw',
            'Origin': 'https://build.hsinchu.gov.tw',
            'Referer': 'https://build.hsinchu.gov.tw/bupic/preLoginFormAction.do',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

    async def fetch_data(
        self,
        on_progress: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[Dict, None]:
        """爬取新竹縣資料"""

        # 先訪問首頁取得 cookies
        self.log(f"正在初始化 {self.city_name} 連線...", on_progress)

        try:
            init_response = await self.client.get("https://build.hsinchu.gov.tw/bupic/preLoginFormAction.do")
            cookies = "; ".join([f"{k}={v}" for k, v in self.client.cookies.items()])
        except httpx.HTTPError as e:
            self.log(f"初始化連線失敗: {e}", on_progress)
            return

        headers = self._get_headers(cookies)
        total_count = 0

        # 逐日爬取
        for cur_date in generate_dates(self.start_date, self.end_date):
            self.log(f"正在爬取 {cur_date}...", on_progress)

            payload = {
                '_search': 'false',
                'nd': str(int(datetime.now().timestamp() * 1000)),
                'rows': '200',
                'page': '1',
                'sidx': '',
                'sord': 'asc',
                'inputcode': '0',
                'code': '0',
                'qtype': '5',
                'regdat': tw_year_format(cur_date)
            }

            try:
                response = await self.client.post(
                    self.base_url,
                    headers=headers,
                    data=payload
                )
                response.raise_for_status()

                # 解析 JSONP 回應 (去掉 "ok(" 和 ")")
                text = response.text
                if text.startswith('ok(') and text.endswith(')'):
                    text = text[3:-1]

                data = json.loads(text)
                rows = data.get('rows', [])

                for row in rows:
                    index_key = row.get('index_key', '')
                    item_date = tw_str_to_date(row.get('identify_lice_date', ''))

                    if not index_key:
                        continue

                    # 取得詳細資料
                    try:
                        detail_url = f"{self.detail_url}?INDEX_KEY={index_key}"
                        detail_headers = self._get_headers(cookies)
                        detail_headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9'

                        detail_response = await self.client.post(
                            detail_url,
                            headers=detail_headers,
                            data={'key': index_key}
                        )
                        detail_response.raise_for_status()

                        # 解析 HTML
                        parsed_data = hc_parse_html_bs4(detail_response.text)
                        refined_data = refine_hsinchu_county(parsed_data, index_key)

                        if refined_data:
                            total_count += 1
                            yield {
                                'date': item_date or cur_date,
                                'index_key': index_key,
                                'data': refined_data
                            }

                    except Exception as e:
                        self.log(f"取得詳細資料失敗 ({index_key}): {e}", on_progress)
                        continue

            except httpx.HTTPError as e:
                self.log(f"HTTP 請求錯誤 ({cur_date}): {e}", on_progress)
                continue
            except json.JSONDecodeError as e:
                self.log(f"JSON 解析錯誤 ({cur_date}): {e}", on_progress)
                continue
            except Exception as e:
                self.log(f"處理資料時發生錯誤 ({cur_date}): {e}", on_progress)
                continue

        self.log(f"{self.city_name} 爬取完成，共 {total_count} 筆資料", on_progress)
