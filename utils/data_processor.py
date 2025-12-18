"""
資料處理工具函式
從原始 Scrapy 專案的 utils.py 移植而來
"""
from datetime import date, datetime, timedelta
import math
import re
from typing import Dict, List, Optional, Tuple, Any
from bs4 import BeautifulSoup


def dict_rename(d: Dict, old_key: str, new_key: str) -> None:
    """將字典中的某個鍵名替換為新的鍵名"""
    replacement = {old_key: new_key}
    for k, v in list(d.items()):
        d[replacement.get(k, k)] = d.pop(k)


def generate_url_params(license_type: str = "使用執照", start: int = 1, year: int = 112) -> str:
    """生成 MCGBM 系統查詢 URL 的參數字串"""
    return f"d=OPENDATA&c=BUILDLIC&Start={start}&執照類別={license_type}&發照日期={year}年"


def tw_str_to_date(date_str: str) -> Optional[date]:
    """將民國日期字串轉換為 Python 的日期物件"""
    if not date_str:
        return None
    keys = re.findall(r'\d+', date_str)
    try:
        if len(keys) == 2:
            return date(int(keys[0]) + 1911, int(keys[1]), 1)
        else:
            return date(int(keys[0]) + 1911, int(keys[1]), int(keys[2]))
    except:
        return None


def tw_year_format(d: date, slash: bool = False) -> str:
    """將日期轉換為民國格式"""
    sep = '/' if slash else ''
    ymd_arr = [f"{d.year - 1911}", f"{d.month:02d}", f"{d.day:02d}"]
    return sep.join(ymd_arr)


def generate_dates(start_date: date, end_date: date, delta: timedelta = timedelta(days=1)):
    """生成日期範圍的生成器"""
    cur_date = start_date
    while cur_date <= end_date:
        yield cur_date
        cur_date += delta


def total_days(start_date: date, end_date: date, delta: timedelta = timedelta(days=1)) -> int:
    """計算日期範圍內的總天數"""
    return math.ceil((end_date - start_date) / delta)


def clean_key(key: Optional[str]) -> str:
    """清理鍵名中的特殊字元"""
    if key is None:
        return ''
    key = key.replace("\u3000", "")
    key = key.replace("：", "")
    key = key.replace('* * *', '')
    key = key.replace('＊＊＊', '')
    key = key.replace('***', '')
    key = key.replace('年月日', '')
    key = key.replace("（", "(")
    key = key.replace("）", ")")
    key = key.replace('址址', '地址')
    key = key.replace('面績', '面積')
    key = key.strip()
    if key in ['-', '', None]:
        return ''
    return key


def check_license_type(s: str) -> str:
    """檢查執照類型"""
    if "造字" in s or "建字" in s or "建造" in s:
        return "建造執照"
    elif ("使字" in s and "變使字" not in s) or "使用執照" in s or "用字" in s:
        return "使用執照"
    else:
        return "其它"


# 預編譯正則表達式
buildings_pattern = re.compile(r'(\d+)棟')
heads_pattern = re.compile(r'(\d+)幢')
floors_pattern = re.compile(r'地上(\d+)層')
basements_pattern = re.compile(r'地下(\d+)層')
units_pattern = re.compile(r'(\d+)戶')
numerical_pattern = re.compile(r'\(\$([\d,]+)\)')


def parse_building_info(s: str) -> Tuple[int, int, int, int, int]:
    """解析建築資訊：棟數、幢數、地上層數、地下層數、戶數"""
    if not s:
        return 0, 0, 0, 0, 0

    buildings_match = buildings_pattern.search(s)
    heads_match = heads_pattern.search(s)
    floors_match = floors_pattern.search(s)
    basements_match = basements_pattern.search(s)
    units_match = units_pattern.search(s)

    buildings = int(buildings_match.group(1)) if buildings_match else 0
    heads = int(heads_match.group(1)) if heads_match else 0
    floors = int(floors_match.group(1)) if floors_match else 0
    basements = int(basements_match.group(1)) if basements_match else 0
    units = int(units_match.group(1)) if units_match else 0

    return buildings, heads, floors, basements, units


def extract_numerical_value(s: str) -> Any:
    """提取數值（工程造價等）"""
    if not s:
        return ""
    match = numerical_pattern.search(s)
    if match:
        price = match.group(1).replace(',', '')
        try:
            price = round(float(price))
        except:
            price = ""
    else:
        price = ""
    return price


# ============== MCGBM 系統資料處理 ==============

def refine_mcbgm(data: Dict) -> Dict:
    """MCGBM 系統資料標準化"""
    license_type = check_license_type(data.get("執照類別", ""))

    building_usage = set()
    if '建築物用途' in data:
        building_usage.update(data['建築物用途'].split(', '))

    for usage in data.get('樓層概要', []):
        if '樓層用途' in usage:
            building_usage.update(usage['樓層用途'].split('、'))
    building_usage = str(list(building_usage))

    price = data.get('工程造價', '')
    try:
        price = round(float(price))
    except:
        price = ""

    def safe_int(val: str) -> int:
        """安全地從字串提取整數"""
        if not val:
            return 0
        try:
            return int(val[:-1]) if val.endswith(('棟', '層', '戶')) else int(val)
        except:
            return 0

    refined_data = {
        "_id": data.get('_id', {}).get('$oid', ''),
        "發照日期": clean_key(data.get('發照日期', '')),
        "執照類別": license_type,
        "使照掛號日期": "-",
        "開工日期": clean_key(data.get('實際開工日期', '')),
        "竣工日期": clean_key(data.get('竣工日期', '')),
        "竣工期限": "-",
        "竣工展期至": "-",
        "申報進度": "-",
        "核發執照字號": clean_key(data.get('核發執照字號', '')),
        "原領執照字號": clean_key(data.get('原領執照字號', '')),
        "變更設計次數": safe_int(str(data.get('變更設計次數', 0))),
        "基地面積": data.get('基地面積', ''),
        "建築面積": data.get('建築面積', ''),
        "總樓地板面積": data.get('總樓地板面積', ''),
        "設計建蔽率": '-',
        "設計容積率": '-',
        "建築物高度": data.get('建築物高度', ''),
        "地下避難面積": data.get('地下避難面積', ''),
        "法定空地面積": data.get('法定空地面積', ''),
        "建造類別": data.get('建造類別', ''),
        "構造別": data.get('構造別', ''),
        "棟數": safe_int(data.get('棟數', '')),
        "幢數": '-',
        "地上層數": safe_int(data.get('地上層數', '')),
        "地下層數": safe_int(data.get('地下層數', '')),
        "戶數": safe_int(data.get('戶數', '')),
        "起造人代表人": clean_key(data.get('起造人代表人', '')),
        "設計人": clean_key(data.get('設計人', '')),
        "設計人事務所": "-",
        "監造人": clean_key(data.get('監造人', '')),
        "監造人事務所": "-",
        "承造人": clean_key(data.get('承造人', '')),
        "承造人營造廠": "-",
        "土地使用分區": clean_key(data.get('土地使用分區', '')),
        "建築物用途": clean_key(building_usage),
        "工程造價": price,
        "樓層概要": clean_key(str(data.get('樓層概要', []))),
        "地號": clean_key(str(data.get('地號', []))),
        "門牌": clean_key(str(data.get('門牌', []))),
        "備註資料": "-",
    }
    return refined_data


# ============== 高雄市資料處理 ==============

def refine_kaohsiung_json(data: Dict, index_key: str) -> Optional[Dict]:
    """高雄市資料標準化"""
    license_type = check_license_type(data.get('title', ''))
    if license_type not in ['建造執照', '使用執照']:
        return None
    if data.get('title') not in ['建造執照號碼', '使用執照號碼']:
        return None

    buildings, heads, floors, basements, units = parse_building_info(clean_key(data.get('buildfloor', '')))

    building_usage = set()
    for usage in data.get('stair', []):
        building_usage.update(usage.get('usage_code_desc', '').split('、'))
    building_usage = str(list(building_usage))

    price = extract_numerical_value(data.get('price', ''))

    floor_info = []
    if "stair" in data:
        for ld in data["stair"]:
            floor_info.append({
                "棟別": ld.get("building_no", ""),
                "層別": ld.get("story_code", ""),
                "樓層高度": ld.get("story_height", ""),
                "申請面積": ld.get("story_area", ""),
                "陽台面積": ld.get("veranda_area", ""),
                "露台面積": ld.get("terrace_area", ""),
                "使用類組": ld.get("usage_code_desc", ""),
            })
        floor_info = str(floor_info)

    land_no = data.get("lan", "").split(' ')[0] if data.get("lan") else ""
    if "lan_data" in data:
        land_no = []
        for ld in data["lan_data"]:
            land_no.append(f"{ld.get('dist', '')}{ld.get('section', '')}{ld.get('road', '')}地號")
        land_no = str(land_no)

    addr = data.get("addr", "")
    if 'p01addr' in data:
        addr = [a.get("addr", "") for a in data['p01addr']]
        addr = str(addr)

    note = ""
    if "memo_seq" in data:
        note = [{m.get("dese", "")} for m in data["memo_seq"]]
        note = str(note)

    refined_data = {
        "_id": index_key,
        "發照日期": clean_key(data.get('IDlicedate', '')),
        "執照類別": license_type,
        "使照掛號日期": '-',
        "開工日期": clean_key(data.get('commence_date', '')),
        "竣工日期": clean_key(data.get('complete_date', '')),
        "竣工展期至": '-',
        "申報進度": '-',
        "核發執照字號": clean_key(data.get("license_desc", "")),
        "原領執照字號": clean_key(data.get("license_desc_old", "")),
        "變更設計次數": '-',
        "基地面積": clean_key(data.get("base_area_total", "")),
        "建築面積": clean_key(data.get("building_area_other", "")),
        "總樓地板面積": clean_key(data.get("total_con_area", "")),
        "設計建蔽率": clean_key(data.get("coverrate", "")),
        "設計容積率": clean_key(data.get("spacerate", "")),
        "建築物高度": clean_key(data.get('buildheight', '')),
        "地下避難面積": clean_key(data.get('airraid_d_area', '')),
        "法定空地面積": clean_key(data.get('openspace', '')),
        "建造類別": clean_key(data.get('buildcategory', '')),
        "構造別": clean_key(data.get('buildkind', '')),
        "棟數": buildings,
        "幢數": heads,
        "地上層數": floors,
        "地下層數": basements,
        "戶數": units,
        "起造人代表人": clean_key(data.get('bmp01_name', '')),
        "設計人": clean_key(data.get('bmp02_name', '')),
        "設計人事務所": clean_key(data.get('p02_officename', '')),
        "監造人": clean_key(data.get('bmp03_name', '')),
        "監造人事務所": clean_key(data.get('p03_officename', '')),
        "承造人": clean_key(data.get('bmp04_boss', '')),
        "承造人營造廠": clean_key(data.get('p04_companyname', '')),
        "土地使用分區": clean_key(data.get('lanusage', '')),
        "建築物用途": clean_key(building_usage),
        "工程造價": price,
        "樓層概要": clean_key(str(floor_info)),
        "地號": clean_key(str(land_no)),
        "門牌": clean_key(str(addr)),
        "備註資料": clean_key(str(note)),
    }
    return refined_data


# ============== NBUPIC 系統 HTML 解析 ==============

def parse_html_bs4(html_content: str) -> Dict:
    """使用 BeautifulSoup 解析 NBUPIC 系統的 HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_data = {}

    headers = soup.select('div.main-header')
    if not headers:
        return parsed_data

    # 解析基本資訊
    basic_cols = headers[0].select('td')
    for i in range(0, len(basic_cols), 2):
        if i + 1 < len(basic_cols):
            key = clean_key(basic_cols[i].get_text())
            value = clean_key(basic_cols[i + 1].get_text())
            parsed_data[key] = value

    # 解析額外資訊
    for col in headers[1:]:
        h2 = col.select_one('h2')
        key = clean_key(h2.get_text()) if h2 else ''
        detail_keys = [clean_key(th.get_text()) for th in col.select('thead th')][1:]

        for tbody in col.select('tbody'):
            detail_values = []
            for tr in tbody.select('tr'):
                detail_cols = []
                for td in tr.select('td')[1:]:
                    cleaned = clean_key(td.get_text())
                    detail_cols.append(cleaned)
                if detail_keys and detail_cols:
                    detail_values.append(dict(zip(detail_keys, detail_cols)))
            if key:
                parsed_data[key] = detail_values

    return parsed_data


def parse_additional_bs4(html_content: str) -> Dict:
    """解析 NBUPIC 額外進度資料"""
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_data = {}

    headers = soup.select('div.main-header')
    if not headers:
        return parsed_data

    basic_cols = headers[0].select('td')
    for i in range(0, len(basic_cols), 2):
        if i + 1 < len(basic_cols):
            key = clean_key(basic_cols[i].get_text())
            value = clean_key(basic_cols[i + 1].get_text(strip=True))
            parsed_data[key] = value

    return parsed_data


def refine_nbupic(data: Dict, index_key: str) -> Optional[Dict]:
    """NBUPIC 系統資料標準化"""
    license_type = check_license_type(data.get('執照字號', ''))
    if license_type not in ['建造執照', '使用執照']:
        return None

    buildings, heads, floors, basements, units = parse_building_info(data.get('層棧戶數', ''))

    building_usage = set()
    for usage in data.get('樓層概要資料', []):
        if '使用類組' in usage:
            building_usage.update(usage['使用類組'].split('、'))
    building_usage = str(list(building_usage))

    price = extract_numerical_value(data.get('工程造價', ''))
    floor_info = str(data.get('樓層概要資料', []))
    land_no = data.get("地號", "")
    addr = str(data.get("地址", ""))

    refined_data = {
        "_id": index_key,
        "發照日期": data.get('發照日期', ''),
        "執照類別": license_type,
        "使照掛號日期": '',
        "開工日期": '',
        "竣工日期": '-',
        "竣工期限": '',
        "竣工展期至": '',
        "申報進度": '',
        "核發執照字號": data.get('執照字號', ''),
        "原領執照字號": data.get('原領執照字號', ''),
        "變更設計次數": '-',
        "基地面積": data.get("基地面積(合計)", ""),
        "建築面積": data.get("建築面積(其他)", "") or data.get("建築面積", ""),
        "總樓地板面積": data.get("總樓地板面積", ""),
        "設計建蔽率": data.get("設計建蔽率", ""),
        "設計容積率": data.get('設計容積率', ''),
        "建築物高度": data.get('建物高度', ''),
        "地下避難面積": data.get('防空避難面積(地下)', ''),
        "法定空地面積": data.get('法定空地面積', ''),
        "建造類別": data.get('建造類別', ''),
        "構造別": data.get('構造種類', ''),
        "棟數": buildings,
        "幢數": heads,
        "地上層數": floors,
        "地下層數": basements,
        "戶數": units,
        "起造人代表人": data.get('起造人', ''),
        "設計人": data.get('設計人(姓名)', ''),
        "設計人事務所": data.get('設計人(事務所)', ''),
        "監造人": data.get('監造人(姓名)', ''),
        "監造人事務所": data.get('監造人(事務所)', ''),
        "承造人": data.get('承造人(姓名)', ''),
        "承造人營造廠": data.get('承造人( 營造廠 )', ''),
        "土地使用分區": data.get('使用分區', ''),
        "建築物用途": building_usage,
        "工程造價": price,
        "樓層概要": floor_info,
        "地號": land_no,
        "門牌": addr,
        "備註資料": '-',
    }
    return refined_data


# ============== 新竹縣 HTML 解析 ==============

def hc_extract_section_info_bs4(soup: BeautifulSoup) -> Dict:
    """新竹縣：提取區塊資訊"""
    results = {}
    sections = soup.select('div.tableCon')

    for section in sections:
        spans = section.select('[class^="tit"]')
        spans.reverse()

        def get_branch(idx):
            arr = []
            min_num = 9
            for span in spans[idx:]:
                tit_class = span.get('class', [''])[0]
                try:
                    tit_num = int(tit_class[-2:])
                except:
                    continue
                if tit_num < min_num:
                    min_num = tit_num
                    arr.append(span)
                if tit_num == 1:
                    return list(reversed(arr))
            return list(reversed(arr))

        arrs = []
        for i in range(len(spans)):
            arr = get_branch(i)
            arrs.append(arr)
        arrs.reverse()

        for idx, arr in enumerate(arrs):
            key = []
            for s in arr:
                key.append(clean_key(s.get_text()))

            # 取得下一個兄弟元素
            last_span = arr[-1] if arr else None
            if last_span:
                next_sibling = last_span.find_next_sibling()
                value = clean_key(next_sibling.get_text(strip=True)) if next_sibling else ''
            else:
                value = ''

            key_str = '-'.join(key)
            if (key_str not in results) or results[key_str] == '':
                results[key_str] = value

            if idx == 0:
                results["核發執照字號"] = value
            elif idx == 1:
                results["原領執照字號"] = value

        if '號碼' in results:
            results['建造執照號碼'] = results['號碼']
            del results['號碼']

    return results


def hc_extract_table_data_bs4(soup: BeautifulSoup) -> Dict:
    """新竹縣：提取表格資料"""
    tables = soup.select('table')
    table_data = {}

    for table in tables:
        headers = [clean_key(th.get_text()) for th in table.select('th')]
        rows = []
        all_tr = table.select('tr')

        for row in all_tr[1:]:
            cells = row.select('td')
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = clean_key(cell.get_text())
            row_data.pop('序號', None)
            if row_data:
                rows.append(row_data)

        # 取得前一個兄弟元素作為標題
        prev_sibling = table.find_previous_sibling()
        if prev_sibling:
            key = clean_key(prev_sibling.get_text())
            table_data[key] = rows
        else:
            parent = table.parent
            if parent:
                prev_parent_sibling = parent.find_previous_sibling()
                if prev_parent_sibling:
                    key = clean_key(prev_parent_sibling.get_text())
                    if key in table_data:
                        merged_rows = []
                        for a, b in zip(rows, table_data[key]):
                            merged_rows.append({**a, **b})
                        table_data[key] = merged_rows
                    else:
                        table_data[key] = rows

    return table_data


def hc_parse_html_bs4(html_content: str) -> Dict:
    """新竹縣：解析 HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    return {
        "key_value_pairs": hc_extract_section_info_bs4(soup),
        "tabular_data": hc_extract_table_data_bs4(soup)
    }


def refine_hsinchu_county(data: Dict, index_key: str) -> Optional[Dict]:
    """新竹縣資料標準化"""
    kv = data.get('key_value_pairs', {})
    td = data.get('tabular_data', {})

    license_type = check_license_type(kv.get('核發執照字號', ''))
    if license_type not in ['建造執照', '使用執照']:
        return None

    new_license = kv.get('核發執照字號', '')
    org_license = kv.get('原領執照字號', '')

    if license_type == '建造執照':
        start_date = ''
        end_date = ''
    else:
        start_date = kv.get('建築執照-開工日期', '')
        end_date = kv.get('建築執照-開工日期-竣工日期', '')

    applicant = kv.get('起造人-姓名', '')
    if '起造人-姓名-事務所' in kv:
        applicant += f" ({kv['起造人-姓名-事務所']})"

    designer = kv.get('設計人-姓名', '')
    designer_office = kv.get('設計人-姓名-事務所', '')
    supervisor = kv.get('監造人-姓名', '')
    supervisor_office = kv.get('監造人-姓名-事務所', '')
    contractor = kv.get('承造人-姓名', '')
    contractor_office = kv.get('承造人-姓名-營造廠', '')

    buildings, heads, floors, basements, units = parse_building_info(kv.get('建物概要-層棧戶數', ''))

    building_usage = set()
    if '樓層概要資料' in td:
        for usage in td['樓層概要資料']:
            building_usage.update(usage.get('使用類組', '').split('、'))
    building_usage = str(list(building_usage))

    price = extract_numerical_value(kv.get('建物概要-工程造價', ''))
    floor_info = str(td.get('樓層概要資料', [])) if '樓層概要資料' in td else ""
    land_no = kv.get('基地概要-地號', '')
    addr = kv.get('基地概要-地址', '')

    note = ""
    if '備註資料' in td:
        note = [{i: n.get("內容", "")} for i, n in enumerate(td['備註資料'])]
        note = str(note)

    refined_data = {
        "_id": index_key,
        "發照日期": clean_key(kv.get('建物概要-發照日期', '')),
        "執照類別": license_type,
        "使照掛號日期": '-',
        "開工日期": clean_key(start_date),
        "竣工日期": clean_key(end_date),
        "竣工期限": '-',
        "竣工展期至": '-',
        "申報進度": '-',
        "核發執照字號": new_license,
        "原領執照字號": org_license,
        "變更設計次數": '-',
        "基地面積": clean_key(kv.get('基地概要-基地面積-退縮地-合計', '')),
        "建築面積": clean_key(kv.get('建物概要-建築面積-騎樓面積-其他', '')),
        "總樓地板面積": clean_key(kv.get('建物概要-設計建蔽率-總樓地板面積', '')),
        "設計建蔽率": '-',
        "設計容積率": '-',
        "建築物高度": clean_key(kv.get('建物概要-設計容積率-建物高度', '')),
        "地下避難面積": clean_key(kv.get('建物概要-防空避難面積-地上-地下', '')),
        "法定空地面積": clean_key(kv.get('建物概要-層棧戶數-法定空地面積', '')),
        "建造類別": clean_key(kv.get('建物概要-建造類別', '')),
        "構造別": clean_key(kv.get('建物概要-建造類別-構造種類', '')),
        "棟數": buildings,
        "幢數": heads,
        "地上層數": floors,
        "地下層數": basements,
        "戶數": units,
        "起造人代表人": clean_key(applicant),
        "設計人": clean_key(designer),
        "設計人事務所": clean_key(designer_office),
        "監造人": clean_key(supervisor),
        "監造人事務所": clean_key(supervisor_office),
        "承造人": clean_key(contractor),
        "承造人營造廠": clean_key(contractor_office),
        "土地使用分區": clean_key(kv.get('基地概要-使用分區', '')),
        "建築物用途": clean_key(building_usage),
        "工程造價": price,
        "樓層概要": clean_key(floor_info),
        "地號": clean_key(land_no),
        "門牌": clean_key(addr),
        "備註資料": clean_key(note),
    }
    return refined_data
