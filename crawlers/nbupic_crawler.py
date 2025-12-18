"""
NBUPIC 系統爬蟲
支援城市：竹科、中科、南科、台南市
"""
import re
from datetime import date
from typing import AsyncGenerator, Dict, Callable, Optional
import httpx

from .base import BaseCrawler
from utils.data_processor import (
    generate_dates,
    tw_year_format,
    parse_html_bs4,
    parse_additional_bs4,
    refine_nbupic
)


class NBUPICCrawler(BaseCrawler):
    """NBUPIC 系統爬蟲"""

    def __init__(
        self,
        city_name: str,
        organ: str,
        start_date: date,
        end_date: date
    ):
        super().__init__(city_name, start_date, end_date)
        self.organ = organ
        self.base_url = f"https://cloudbm.nlma.gov.tw/NBUPIC/index.jsp?organ={organ}&QryType=5"

    def _get_headers(self, cookies: str, nbupicpkey: str = "") -> Dict:
        """取得請求標頭"""
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookies,
            'Host': 'cloudbm.nlma.gov.tw',
            'Origin': 'https://cloudbm.nlma.gov.tw',
            'Referer': f'https://cloudbm.nlma.gov.tw/NBUPIC/index.jsp?organ={self.organ}&QryType=5',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        if nbupicpkey:
            headers['Nbupicpkey'] = nbupicpkey
        return headers

    async def fetch_data(
        self,
        on_progress: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[Dict, None]:
        """爬取 NBUPIC 系統資料"""

        # Step 1: 初始化，取得 cookies 和 token
        self.log(f"正在初始化 {self.city_name} 連線...", on_progress)

        try:
            init_headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.google.com/',
            }
            init_response = await self.client.get(self.base_url, headers=init_headers)
            init_response.raise_for_status()

            cookies = "; ".join([f"{k}={v}" for k, v in self.client.cookies.items()])

            # 提取 queryID
            query_id_match = re.search(r'name="frm_query_para_PRIMARYID"\s+value="([^"]+)"', init_response.text)
            query_id = query_id_match.group(1) if query_id_match else ""

            # 提取 Nbupicpkey
            nbupicpkey_match = re.search(r'sidjphumrsqf="([^"]+)"', init_response.text)
            nbupicpkey = nbupicpkey_match.group(1) if nbupicpkey_match else ""

            if not query_id or not nbupicpkey:
                self.log("無法取得必要的認證資訊", on_progress)
                return

        except httpx.HTTPError as e:
            self.log(f"初始化連線失敗: {e}", on_progress)
            return

        headers = self._get_headers(cookies, nbupicpkey)
        total_count = 0

        # Step 2: 逐日爬取
        for cur_date in generate_dates(self.start_date, self.end_date):
            self.log(f"正在爬取 {cur_date}...", on_progress)

            # 發送查詢請求
            query_url = "https://cloudbm.nlma.gov.tw/NBUPIC/nbupic_lst.jsp?queryparammode=true"
            payload = {
                'Qry_LICENSING_UNIT': self.organ,
                'Qry_QryType': '5',
                'Qry_license_yy': '',
                'RC_Qry_regdat': tw_year_format(cur_date, slash=True),
                'Qry_regdat': cur_date.strftime('%Y%m%d'),
                'Qry_imageCodetxt': '',
                'frm_query_para_PRIMARYID': query_id,
                'frm_query_para_sortKeys': 'null',
                'fromajax': 'true',
                'QueryParamButton_executeQuery': '執行查詢'
            }

            try:
                # 先發送一次查詢
                await self.client.post(query_url, headers=headers, data=payload)

                # 擴展結果數量到 200
                expand_url = f"https://cloudbm.nlma.gov.tw/NBUPIC/nbupic_lst.jsp?queryparammode=true&&cur_pagesize=200&frm_query_para_PRIMARYID={query_id}"
                expand_response = await self.client.post(expand_url, headers=headers, data=payload)
                expand_response.raise_for_status()

                # 提取所有 IndexKey
                keys = re.findall(r"run_button\('([^']+)'", expand_response.text)

                for key in keys:
                    try:
                        # 取得詳細資料
                        lic_url = "https://cloudbm.nlma.gov.tw/NBUPIC/licInfo.jsp?"
                        lic_payload = {
                            'IndexKey': key,
                            'organ': self.organ,
                            'responseText': 'true'
                        }
                        lic_response = await self.client.post(lic_url, headers=headers, data=lic_payload)
                        lic_response.raise_for_status()

                        # 解析基本資料
                        parsed_data = parse_html_bs4(lic_response.text)
                        refined_data = refine_nbupic(parsed_data, key)

                        if refined_data:
                            # 取得進度資料
                            sched_url = "https://cloudbm.nlma.gov.tw/NBUPIC/schedule.jsp?"
                            sched_response = await self.client.post(sched_url, headers=headers, data=lic_payload)
                            sched_response.raise_for_status()

                            additional_data = parse_additional_bs4(sched_response.text)

                            # 更新進度欄位
                            refined_data['使照掛號日期'] = additional_data.get('使照掛號日期', '')
                            refined_data['開工日期'] = additional_data.get('開工日期', '')
                            refined_data['竣工期限'] = additional_data.get('竣工期限', '')
                            refined_data['竣工展期至'] = additional_data.get('竣工展期至', '')
                            refined_data['申報進度'] = additional_data.get('申報進度', '')

                            total_count += 1
                            yield {
                                'date': cur_date,
                                'index_key': key,
                                'data': refined_data
                            }

                    except Exception as e:
                        self.log(f"取得詳細資料失敗 ({key}): {e}", on_progress)
                        continue

            except httpx.HTTPError as e:
                self.log(f"HTTP 請求錯誤 ({cur_date}): {e}", on_progress)
                continue
            except Exception as e:
                self.log(f"處理資料時發生錯誤 ({cur_date}): {e}", on_progress)
                continue

        self.log(f"{self.city_name} 爬取完成，共 {total_count} 筆資料", on_progress)
