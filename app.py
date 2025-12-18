"""
建築執照爬蟲系統 - 純 Streamlit 版
不需要 FastAPI，一個程式搞定
"""
import streamlit as st
import asyncio
import os
import sys
from datetime import date, timedelta, datetime

# 加入模組路徑
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crawlers import get_crawler, CITY_CONFIG
from utils.excel_writer import ExcelWriter

# 頁面設定
st.set_page_config(
    page_title="建築執照爬蟲系統",
    page_icon="🏛️",
    layout="wide"
)

# 輸出目錄
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_async(coro):
    """執行異步函式"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def crawl_city(city: str, start_date: date, end_date: date, log_placeholder, status_placeholder):
    """爬取單一城市資料"""
    logs = []

    def log_callback(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {msg}"
        logs.append(log_msg)
        # 更新日誌顯示（只顯示最新 15 條）
        log_placeholder.code("\n".join(logs[-15:]), language=None)

    crawler = get_crawler(city, start_date, end_date)
    excel_writer = ExcelWriter(city, OUTPUT_DIR)

    total_records = 0
    async with crawler:
        async for item in crawler.fetch_data(on_progress=log_callback):
            excel_writer.write_item(item)
            total_records += 1
            status_placeholder.text(f"已爬取 {total_records} 筆資料")

    saved_files = excel_writer.save()
    log_callback(f"完成！共 {total_records} 筆資料")

    return saved_files, total_records


# ============== 主介面 ==============

st.title("🏛️ 建築執照爬蟲系統")

# 側邊欄：設定區
with st.sidebar:
    st.header("📍 選擇縣市")

    # 按系統分類城市
    mcgbm_cities = ["基隆市", "新北市", "桃園市", "新竹市", "台中市"]
    nbupic_cities = ["竹科", "中科", "南科", "台南市"]
    other_cities = ["新竹縣", "高雄市"]

    st.subheader("MCGBM 系統")
    selected_mcgbm = st.multiselect(
        "MCGBM 城市",
        options=mcgbm_cities,
        default=[],
        key="mcgbm",
        label_visibility="collapsed"
    )

    st.subheader("NBUPIC 系統")
    selected_nbupic = st.multiselect(
        "NBUPIC 城市",
        options=nbupic_cities,
        default=[],
        key="nbupic",
        label_visibility="collapsed"
    )

    st.subheader("其他系統")
    selected_other = st.multiselect(
        "其他城市",
        options=other_cities,
        default=[],
        key="other",
        label_visibility="collapsed"
    )

    selected_cities = selected_mcgbm + selected_nbupic + selected_other

    st.divider()

    st.header("📅 選擇日期範圍")
    start_date = st.date_input(
        "起始日期",
        value=date.today() - timedelta(days=7),
        key="start_date"
    )
    end_date = st.date_input(
        "結束日期",
        value=date.today(),
        key="end_date"
    )

    # 驗證日期
    date_valid = end_date >= start_date
    if not date_valid:
        st.error("結束日期必須大於等於開始日期")

# 主區域
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📋 已選擇的城市")
    if selected_cities:
        st.write(", ".join(selected_cities))
    else:
        st.info("請在左側選擇至少一個城市")

with col2:
    st.subheader("📆 日期範圍")
    st.write(f"{start_date} ~ {end_date}")
    days = (end_date - start_date).days + 1
    st.write(f"共 {days} 天")

st.divider()

# 初始化 session state
if 'results' not in st.session_state:
    st.session_state.results = {}
if 'running' not in st.session_state:
    st.session_state.running = False

# 開始爬取
can_start = bool(selected_cities) and date_valid and not st.session_state.running

if st.button("🚀 開始爬取", type="primary", use_container_width=True, disabled=not can_start):
    st.session_state.running = True
    st.session_state.results = {}

    total_cities = len(selected_cities)

    # 進度顯示
    overall_progress = st.progress(0, text="準備中...")

    for i, city in enumerate(selected_cities):
        st.subheader(f"🏙️ {city}")

        col1, col2 = st.columns([3, 1])
        with col1:
            log_placeholder = st.empty()
        with col2:
            status_placeholder = st.empty()

        # 更新整體進度
        overall_progress.progress(
            i / total_cities,
            text=f"正在處理 {city} ({i+1}/{total_cities})"
        )

        try:
            # 執行爬蟲
            saved_files, total_records = run_async(
                crawl_city(city, start_date, end_date, log_placeholder, status_placeholder)
            )

            st.session_state.results[city] = {
                'files': saved_files,
                'records': total_records
            }

            st.success(f"✅ {city} 完成！共 {total_records} 筆資料")

        except Exception as e:
            st.error(f"❌ {city} 發生錯誤: {e}")
            st.session_state.results[city] = {
                'files': {},
                'records': 0,
                'error': str(e)
            }

    overall_progress.progress(1.0, text="全部完成！")
    st.session_state.running = False
    st.balloons()

# 下載區
if st.session_state.results:
    st.divider()
    st.header("💾 下載結果")

    for city, result in st.session_state.results.items():
        if result.get('files'):
            st.subheader(f"📁 {city}")
            col1, col2, col3 = st.columns([2, 2, 1])

            files = result['files']

            with col1:
                if '建造執照' in files and os.path.exists(files['建造執照']):
                    with open(files['建造執照'], 'rb') as f:
                        st.download_button(
                            label=f"📥 {city}_建造執照.xlsx",
                            data=f.read(),
                            file_name=f"{city}_建造執照.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{city}_build"
                        )

            with col2:
                if '使用執照' in files and os.path.exists(files['使用執照']):
                    with open(files['使用執照'], 'rb') as f:
                        st.download_button(
                            label=f"📥 {city}_使用執照.xlsx",
                            data=f.read(),
                            file_name=f"{city}_使用執照.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{city}_use"
                        )

            with col3:
                st.metric("資料筆數", result.get('records', 0))

    # 清除結果
    if st.button("🗑️ 清除結果"):
        st.session_state.results = {}
        st.rerun()

# 頁尾
st.divider()
st.caption("建築執照爬蟲系統 v1.0 | Streamlit")
