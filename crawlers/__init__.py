# 爬蟲模組
from .base import BaseCrawler
from .mcgbm_crawler import MCGBMCrawler
from .nbupic_crawler import NBUPICCrawler
from .hsinchu_crawler import HsinchuCountyCrawler
from .kaohsiung_crawler import KaohsiungCrawler


# 城市配置
CITY_CONFIG = {
    # MCGBM 系統
    "基隆市": {
        "crawler_class": MCGBMCrawler,
        "base_url": "https://master.klcg.gov.tw/opendata/OpenDataSearchUrl.do?",
        "system": "MCGBM"
    },
    "新北市": {
        "crawler_class": MCGBMCrawler,
        "base_url": "https://building-apply.publicwork.ntpc.gov.tw/opendata/OpenDataSearchUrl.do?",
        "system": "MCGBM"
    },
    "桃園市": {
        "crawler_class": MCGBMCrawler,
        "base_url": "https://building.tycg.gov.tw/opendata/OpenDataSearchUrl.do?",
        "system": "MCGBM"
    },
    "新竹市": {
        "crawler_class": MCGBMCrawler,
        "base_url": "https://build.hccg.gov.tw/opendata/OpenDataSearchUrl.do?",
        "system": "MCGBM"
    },
    "台中市": {
        "crawler_class": MCGBMCrawler,
        "base_url": "https://mcgbm.taichung.gov.tw/opendata/OpenDataSearchUrl.do?",
        "system": "MCGBM"
    },
    # NBUPIC 系統
    "竹科": {
        "crawler_class": NBUPICCrawler,
        "organ": "B10",
        "system": "NBUPIC"
    },
    "中科": {
        "crawler_class": NBUPICCrawler,
        "organ": "B20",
        "system": "NBUPIC"
    },
    "南科": {
        "crawler_class": NBUPICCrawler,
        "organ": "B30",
        "system": "NBUPIC"
    },
    "台南市": {
        "crawler_class": NBUPICCrawler,
        "organ": "IF0",
        "system": "NBUPIC"
    },
    # 獨立系統
    "新竹縣": {
        "crawler_class": HsinchuCountyCrawler,
        "system": "HsinchuCounty"
    },
    "高雄市": {
        "crawler_class": KaohsiungCrawler,
        "system": "Kaohsiung"
    },
}


def get_crawler(city_name: str, start_date, end_date):
    """取得指定城市的爬蟲實例"""
    if city_name not in CITY_CONFIG:
        raise ValueError(f"不支援的城市: {city_name}")

    config = CITY_CONFIG[city_name]
    crawler_class = config["crawler_class"]

    if config["system"] == "MCGBM":
        return crawler_class(
            city_name=city_name,
            base_url=config["base_url"],
            start_date=start_date,
            end_date=end_date
        )
    elif config["system"] == "NBUPIC":
        return crawler_class(
            city_name=city_name,
            organ=config["organ"],
            start_date=start_date,
            end_date=end_date
        )
    else:
        return crawler_class(
            city_name=city_name,
            start_date=start_date,
            end_date=end_date
        )


def get_supported_cities():
    """取得支援的城市列表"""
    return list(CITY_CONFIG.keys())
