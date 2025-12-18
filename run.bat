@echo off
chcp 65001 >nul
echo ========================================
echo   建築執照爬蟲系統 - 啟動腳本
echo ========================================
echo.

cd /d %~dp0

REM 檢查是否有虛擬環境
if not exist ".venv" (
    echo [1/3] 建立虛擬環境...
    uv venv
)

echo [2/3] 安裝相依套件...
uv pip install -r requirements.txt

echo.
echo [3/3] 啟動 Streamlit...
echo.
echo 網址: http://localhost:8501
echo.

.venv\Scripts\streamlit run app.py --server.port 8501
