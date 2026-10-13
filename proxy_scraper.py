#!/usr/bin/env python3
"""
代理服务器抓取与验证工具
Proxy Scraper & Verifier Tool

一键运行：python proxy_scraper.py
  - 自动抓取所有公开数据源
  - 自动并发验证可用性（真实流量测试）
  - 自动查询 IP 地理位置
  - 输出 proxies.txt（标准格式：protocol://ip:port）
  - 输出 proxies.csv（详细信息：地址/出口IP/位置/延迟/协议）
"""

import argparse
import csv
import json
import os
import re
import socket
import struct
import sys
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass


# ==================== 数据源配置 ====================
# GitHub 镜像前缀（国内可访问）
GITHUB_MIRRORS = [
    "",  # 直连（备用）
    "https://ghproxy.net/",
    "https://mirror.ghproxy.com/",
    "https://gh-proxy.com/",
    "https://ghfast.top/",
]

# 原始 GitHub 代理列表
GITHUB_SOURCES = [
    # TheSpeedX/PROXY-List
    {"url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "protocol": "socks5"},
    # monosans/proxy-list
    {"url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    # clarketm/proxy-list
    {"url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "protocol": "http"},
    # shiftytr/proxy-list
    {"url": "https://raw.githubusercontent.com/shiftytr/proxy-list/master/proxy.txt", "protocol": "http"},
    # jetkai/proxy-list
    {"url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "protocol": "socks5"},
    # hookzof/socks5_list
    {"url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "protocol": "socks5"},
    # mmpx12/proxy-list
    {"url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "protocol": "socks5"},
    # roosterkid/openproxylist
    {"url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "protocol": "socks5"},
    # mertguvencli/http-proxy-list
    {"url": "https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt", "protocol": "http"},
    # Zaeem20/FREE_PROXIES_LIST
    {"url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt", "protocol": "socks5"},
    # hyperbeats/proxy-list
    {"url": "https://raw.githubusercontent.com/hyperbeats/proxy-list/main/http.txt", "protocol": "http"},
    # UptimerBot/proxy-list
    {"url": "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"url": "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/socks4.txt", "protocol": "socks4"},
    {"url": "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    # officialputuid/free-proxy
    {"url": "https://raw.githubusercontent.com/officialputuid/free-proxy/main/proxy_list.txt", "protocol": "http"},
    # a2u/free-proxy-list
    {"url": "https://raw.githubusercontent.com/a2u/free-proxy-list/master/free-proxy-list.txt", "protocol": "http"},
    # enwaiax/proxy-list
    {"url": "https://raw.githubusercontent.com/enwaiax/proxy-list/main/proxy-list.txt", "protocol": "http"},
    # yemixzy/proxy-list
    {"url": "https://raw.githubusercontent.com/yemixzy/proxy-list/master/proxy-list/data.txt", "protocol": "http"},
    # fate0/proxylist
    {"url": "https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list", "protocol": "http"},
]

# 非 GitHub 的直接代理源（国内可访问）
DIRECT_SOURCES = [
    {"url": "https://www.proxy-list.download/api/v1/get?type=https", "protocol": "http"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=http", "protocol": "http"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=socks4", "protocol": "socks4"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=socks5", "protocol": "socks5"},
    {"url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all", "protocol": "http"},
    {"url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=5000&country=all", "protocol": "socks4"},
    {"url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all", "protocol": "socks5"},
    {"url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "protocol": "http"},
]

# 全部数据源
PROXY_SOURCES = DIRECT_SOURCES + GITHUB_SOURCES

# 测试端点：api.ip.sb 返回纯文本出口 IP
# P1-1 修复：延迟 DNS 解析，避免没网时启动就崩
TEST_HOST = "api.ip.sb"
TEST_PATH = "/ip"
_TEST_IP_CACHE = None

def _get_test_ip() -> str:
    """延迟解析测试服务器 IP（带缓存）"""
    global _TEST_IP_CACHE
    if _TEST_IP_CACHE is None:
        try:
            _TEST_IP_CACHE = socket.gethostbyname(TEST_HOST)
        except Exception:
            _TEST_IP_CACHE = TEST_HOST  # 解析失败直接用域名
    return _TEST_IP_CACHE

# 兼容旧代码的引用方式
class _LazyIP:
    def __str__(self):
        return _get_test_ip()
    def __eq__(self, other):
        return _get_test_ip() == other
    def __hash__(self):
        return hash(_get_test_ip())
    def __add__(self, other):
        return _get_test_ip() + other
    def __radd__(self, other):
        return other + _get_test_ip()

TEST_IP = _LazyIP()

# 真实网站测速目标（测真实网页加载延迟）
REAL_TEST_SITES = [
    {"host": "www.baidu.com", "path": "/", "port": 80},
    {"host": "www.qq.com", "path": "/", "port": 80},
]


# ==================== 国家名称中英对照 ====================
COUNTRY_CN = {
    "China": "中国", "Taiwan": "中国台湾", "Hong Kong": "中国香港", "Macau": "中国澳门",
    "United States": "美国", "USA": "美国", "United States of America": "美国",
    "Japan": "日本", "South Korea": "韩国", "Korea": "韩国", "Republic of Korea": "韩国",
    "Singapore": "新加坡", "Malaysia": "马来西亚", "Thailand": "泰国", "Vietnam": "越南",
    "Indonesia": "印度尼西亚", "Philippines": "菲律宾", "India": "印度",
    "Russia": "俄罗斯", "Russian Federation": "俄罗斯",
    "United Kingdom": "英国", "UK": "英国", "Great Britain": "英国",
    "Germany": "德国", "France": "法国", "Netherlands": "荷兰", "Italy": "意大利",
    "Spain": "西班牙", "Poland": "波兰", "Ukraine": "乌克兰", "Romania": "罗马尼亚",
    "Czech Republic": "捷克", "Czechia": "捷克", "Hungary": "匈牙利", "Sweden": "瑞典",
    "Norway": "挪威", "Finland": "芬兰", "Denmark": "丹麦", "Switzerland": "瑞士",
    "Austria": "奥地利", "Belgium": "比利时", "Portugal": "葡萄牙", "Greece": "希腊",
    "Ireland": "爱尔兰", "Bulgaria": "保加利亚", "Croatia": "克罗地亚", "Serbia": "塞尔维亚",
    "Slovakia": "斯洛伐克", "Lithuania": "立陶宛", "Latvia": "拉脱维亚", "Estonia": "爱沙尼亚",
    "Belarus": "白俄罗斯", "Moldova": "摩尔多瓦", "Georgia": "格鲁吉亚", "Armenia": "亚美尼亚",
    "Kazakhstan": "哈萨克斯坦", "Uzbekistan": "乌兹别克斯坦", "Azerbaijan": "阿塞拜疆",
    "Turkey": "土耳其", "Israel": "以色列", "Saudi Arabia": "沙特阿拉伯",
    "United Arab Emirates": "阿联酋", "UAE": "阿联酋", "Iran": "伊朗", "Iraq": "伊拉克",
    "Egypt": "埃及", "South Africa": "南非", "Nigeria": "尼日利亚", "Kenya": "肯尼亚",
    "Brazil": "巴西", "Argentina": "阿根廷", "Chile": "智利", "Colombia": "哥伦比亚",
    "Mexico": "墨西哥", "Peru": "秘鲁", "Venezuela": "委内瑞拉", "Ecuador": "厄瓜多尔",
    "Canada": "加拿大", "Australia": "澳大利亚", "New Zealand": "新西兰",
    "Pakistan": "巴基斯坦", "Bangladesh": "孟加拉国", "Sri Lanka": "斯里兰卡",
    "Myanmar": "缅甸", "Cambodia": "柬埔寨", "Laos": "老挝", "Mongolia": "蒙古",
    "Nepal": "尼泊尔", "Cyprus": "塞浦路斯", "Iceland": "冰岛", "Luxembourg": "卢森堡",
    "Malta": "马耳他", "Albania": "阿尔巴尼亚", "Bosnia and Herzegovina": "波黑",
    "North Macedonia": "北马其顿", "Slovenia": "斯洛文尼亚", "Montenegro": "黑山",
    "Puerto Rico": "波多黎各", "Jamaica": "牙买加", "Cuba": "古巴",
    "Panama": "巴拿马", "Costa Rica": "哥斯达黎加", "Guatemala": "危地马拉",
    "Dominican Republic": "多米尼加", "Uruguay": "乌拉圭", "Paraguay": "巴拉圭",
    "Bolivia": "玻利维亚", "Morocco": "摩洛哥", "Algeria": "阿尔及利亚",
    "Tunisia": "突尼斯", "Ghana": "加纳", "Tanzania": "坦桑尼亚", "Uganda": "乌干达",
    "Ethiopia": "埃塞俄比亚", "Zimbabwe": "津巴布韦", "Zambia": "赞比亚",
}

# 协议中文名
PROTOCOL_CN = {"http": "HTTP代理", "socks4": "SOCKS4代理", "socks5": "SOCKS5代理"}

# 城市名称中英对照
CITY_CN = {
    "Beijing": "北京", "Shanghai": "上海", "Guangzhou": "广州", "Shenzhen": "深圳",
    "Hangzhou": "杭州", "Nanjing": "南京", "Chengdu": "成都", "Wuhan": "武汉",
    "Tianjin": "天津", "Chongqing": "重庆", "Xian": "西安", "Suzhou": "苏州",
    "Zhengzhou": "郑州", "Changsha": "长沙", "Qingdao": "青岛", "Dalian": "大连",
    "Ningbo": "宁波", "Xiamen": "厦门", "Hefei": "合肥", "Fuzhou": "福州",
    "Jinan": "济南", "Kunming": "昆明", "Shenyang": "沈阳", "Harbin": "哈尔滨",
    "Shijiazhuang": "石家庄", "Nanchang": "南昌", "Guiyang": "贵阳", "Lanzhou": "兰州",
    "Urumqi": "乌鲁木齐", "Hohhot": "呼和浩特", "Yinchuan": "银川", "Xining": "西宁",
    "Nanning": "南宁", "Haikou": "海口", "Lhasa": "拉萨", "Hong Kong": "香港",
    "Macau": "澳门", "Taipei": "台北", "Kaohsiung": "高雄", "Taichung": "台中",
    "Tokyo": "东京", "Osaka": "大阪", "Nagoya": "名古屋", "Sapporo": "札幌",
    "Seoul": "首尔", "Busan": "釜山", "Incheon": "仁川",
    "Singapore": "新加坡", "Kuala Lumpur": "吉隆坡", "Bangkok": "曼谷",
    "Hanoi": "河内", "Ho Chi Minh City": "胡志明市", "Jakarta": "雅加达",
    "Manila": "马尼拉", "Mumbai": "孟买", "Delhi": "新德里", "Bangalore": "班加罗尔",
    "Moscow": "莫斯科", "Saint Petersburg": "圣彼得堡", "Novosibirsk": "新西伯利亚",
    "London": "伦敦", "Manchester": "曼彻斯特", "Birmingham": "伯明翰", "Edinburgh": "爱丁堡",
    "Berlin": "柏林", "Munich": "慕尼黑", "Frankfurt": "法兰克福", "Hamburg": "汉堡",
    "Paris": "巴黎", "Lyon": "里昂", "Marseille": "马赛",
    "Amsterdam": "阿姆斯特丹", "Rotterdam": "鹿特丹",
    "Madrid": "马德里", "Barcelona": "巴塞罗那", "Valencia": "瓦伦西亚",
    "Rome": "罗马", "Milan": "米兰", "Naples": "那不勒斯",
    "Warsaw": "华沙", "Krakow": "克拉科夫",
    "Kyiv": "基辅", "Lviv": "利沃夫", "Bucharest": "布加勒斯特",
    "Prague": "布拉格", "Budapest": "布达佩斯",
    "Stockholm": "斯德哥尔摩", "Gothenburg": "哥德堡",
    "Oslo": "奥斯陆", "Helsinki": "赫尔辛基", "Copenhagen": "哥本哈根",
    "Zurich": "苏黎世", "Geneva": "日内瓦", "Vienna": "维也纳",
    "Brussels": "布鲁塞尔", "Lisbon": "里斯本", "Dublin": "都柏林",
    "Athens": "雅典", "Sofia": "索非亚", "Zagreb": "萨格勒布",
    "Belgrade": "贝尔格莱德", "Bratislava": "布拉迪斯拉发",
    "Vilnius": "维尔纽斯", "Riga": "里加", "Tallinn": "塔林",
    "Minsk": "明斯克", "Tbilisi": "第比利斯", "Almaty": "阿拉木图",
    "Istanbul": "伊斯坦布尔", "Ankara": "安卡拉",
    "Dubai": "迪拜", "Abu Dhabi": "阿布扎比", "Riyadh": "利雅得",
    "Tel Aviv": "特拉维夫", "Jerusalem": "耶路撒冷",
    "Tehran": "德黑兰", "Baghdad": "巴格达", "Cairo": "开罗",
    "Johannesburg": "约翰内斯堡", "Cape Town": "开普敦", "Durban": "德班",
    "Lagos": "拉各斯", "Nairobi": "内罗毕",
    "Sao Paulo": "圣保罗", "Rio de Janeiro": "里约热内卢", "Brasilia": "巴西利亚",
    "Buenos Aires": "布宜诺斯艾利斯", "Santiago": "圣地亚哥",
    "Bogota": "波哥大", "Lima": "利马", "Caracas": "加拉加斯",
    "Mexico City": "墨西哥城", "Toronto": "多伦多", "Vancouver": "温哥华",
    "Montreal": "蒙特利尔", "Ottawa": "渥太华", "Calgary": "卡尔加里",
    "New York": "纽约", "Los Angeles": "洛杉矶", "Chicago": "芝加哥",
    "San Francisco": "旧金山", "Seattle": "西雅图", "Miami": "迈阿密",
    "Boston": "波士顿", "Houston": "休斯顿", "Dallas": "达拉斯",
    "Atlanta": "亚特兰大", "Denver": "丹佛", "Las Vegas": "拉斯维加斯",
    "Washington": "华盛顿", "Philadelphia": "费城", "Phoenix": "凤凰城",
    "Sydney": "悉尼", "Melbourne": "墨尔本", "Brisbane": "布里斯班",
    "Perth": "珀斯", "Auckland": "奥克兰", "Wellington": "惠灵顿",
    "Karachi": "卡拉奇", "Lahore": "拉合尔", "Dhaka": "达卡",
    "Colombo": "科伦坡", "Yangon": "仰光", "Phnom Penh": "金边",
    "Ulaanbaatar": "乌兰巴托", "Kathmandu": "加德满都",
}

# 运营商/ISP 中英对照
ISP_CN = {
    "China Telecom": "中国电信", "China Unicom": "中国联通", "China Mobile": "中国移动",
    "China Tietong": "中国铁通", "China Net": "中国电信", "ChinaNet": "中国电信",
    "CHINANET": "中国电信", "CHINA UNICOM": "中国联通", "CHINA MOBILE": "中国移动",
    "CERNET": "中国教育网", "CSTNET": "中国科技网",
    "Alibaba Cloud": "阿里云", "Alibaba (China) Technology Co., Ltd.": "阿里云",
    "Aliyun Computing Co., Ltd": "阿里云",
    "Tencent Cloud": "腾讯云", "Tencent Cloud Computing Co., Ltd": "腾讯云",
    "Tencent": "腾讯",
    "Huawei Cloud": "华为云", "Huawei Technologies Co.": "华为",
    "Baidu Cloud": "百度云", "Baidu": "百度",
    "JD Cloud": "京东云", "JD.com": "京东",
    "UCloud": "UCloud", "Kingsoft Cloud": "金山云",
    "Amazon.com": "亚马逊AWS", "Amazon Technologies Inc.": "亚马逊AWS",
    "AWS": "亚马逊AWS", "Amazon": "亚马逊AWS",
    "Microsoft Corporation": "微软Azure", "Microsoft": "微软",
    "Google LLC": "谷歌云", "Google Inc.": "谷歌", "Google Cloud": "谷歌云",
    "Google": "谷歌",
    "DigitalOcean": "DigitalOcean", "Linode": "Linode", "Vultr": "Vultr",
    "OVH": "OVH", "OVH SAS": "OVH", "Hetzner": "Hetzner",
    "Hetzner Online GmbH": "Hetzner",
    "Choopa": "Vultr/Choopa", "Constant.com": "Constant",
    "ColoCrossing": "ColoCrossing",
    "Time Warner Cable": "时代华纳", "Comcast": "Comcast",
    "AT&T": "AT&T", "Verizon": "Verizon", "T-Mobile": "T-Mobile",
    "Sprint": "Sprint", "Cox Communications": "Cox",
    "NTT Communications": "NTT", "NTT": "NTT",
    "KDDI": "KDDI", "SoftBank": "软银", "Softbank": "软银",
    "SK Broadband": "SK宽带", "KT": "KT", "LG U+": "LG U+",
    "Singtel": "新加坡电信", "StarHub": "StarHub",
    "Telekom Malaysia": "马来西亚电信", "Maxis": "Maxis",
    "True Internet": "True", "AIS": "AIS",
    "VNPT": "VNPT", "Viettel": "Viettel",
    "Telkom Indonesia": "印尼电信", "IndiHome": "IndiHome",
    "PLDT": "PLDT", "Globe Telecom": "Globe",
    "Bharti Airtel": "Airtel", "Reliance Jio": "Jio",
    "BSNL": "BSNL", "Tata Communications": "Tata",
    "Rostelecom": "俄罗斯电信", "MTS": "MTS", "MegaFon": "MegaFon",
    "Beeline": "Beeline", "ER-Telecom": "ER-Telecom",
    "BT": "英国电信", "Virgin Media": "Virgin Media",
    "Sky Broadband": "Sky", "TalkTalk": "TalkTalk",
    "Deutsche Telekom": "德国电信", "Vodafone": "沃达丰",
    "O2": "O2", "Telefonica": "Telefonica",
    "Orange": "Orange", "Free SAS": "Free",
    "Telecom Italia": "意大利电信", "TIM": "TIM",
    "KPN": "KPN", "Ziggo": "Ziggo",
    "Telefonica de Espana": "西班牙电信", "Movistar": "Movistar",
    "Play": "Play", "Orange Polska": "Orange波兰",
    "Kyivstar": "Kyivstar", "DTEK": "DTEK",
    "Swisscom": "瑞士电信", "Sunrise": "Sunrise",
    "Telenor": "Telenor", "Telia": "Telia", "Elisa": "Elisa",
    "Rogers": "Rogers", "Bell Canada": "Bell", "Telus": "Telus",
    "Shaw Communications": "Shaw",
}


def to_cn_country(name: str) -> str:
    """英文国家名转中文"""
    if not name:
        return ""
    if name in COUNTRY_CN:
        return COUNTRY_CN[name]
    for en, cn in COUNTRY_CN.items():
        if en.lower() == name.lower():
            return cn
    return name


def to_cn_city(name: str) -> str:
    """英文城市名转中文"""
    if not name:
        return ""
    if name in CITY_CN:
        return CITY_CN[name]
    for en, cn in CITY_CN.items():
        if en.lower() == name.lower():
            return cn
    return name


def to_cn_isp(name: str) -> str:
    """英文运营商名转中文"""
    if not name:
        return ""
    if name in ISP_CN:
        return ISP_CN[name]
    for en, cn in ISP_CN.items():
        if en.lower() == name.lower():
            return cn
    # 模糊匹配
    lower = name.lower()
    for en, cn in ISP_CN.items():
        if en.lower() in lower or lower in en.lower():
            return cn
    return name


def get_speed_grade(latency: float) -> str:
    """延迟等级"""
    if latency <= 0:
        return "未知"
    if latency < 1:
        return "极速"
    if latency < 2:
        return "快速"
    if latency < 3:
        return "一般"
    return "较慢"


def get_anonymity_type(proxy) -> str:
    """
    匿名类型三级判定：
    - 高匿：出口IP ≠ 代理IP，且不泄露客户端信息
    - 普匿：出口IP ≠ 代理IP，但泄露了代理标识（Via/X-Forwarded-For）
    - 透明：出口IP = 代理IP（直接暴露你的真实IP）
    """
    if not proxy.exit_ip:
        return "未知"
    if proxy.exit_ip == proxy.host:
        return "透明"
    # 出口 IP 变了，但检查是否泄露了客户端信息
    if getattr(proxy, 'leaks_info', False):
        return "普匿"
    return "高匿"


# 全局：中断标记
_interrupted = False
_partial_results = []

def _signal_handler(sig, frame):
    """Ctrl+C 优雅中断"""
    global _interrupted
    _interrupted = True
    print("\n")
    log("⚠ 收到中断信号，正在停止并保存已找到的代理...", "WARN")


# ==================== 数据结构 ====================
@dataclass
class Proxy:
    host: str
    port: int
    protocol: str
    latency: float = -1.0          # 代理通道延迟（api.ip.sb）
    real_latency: float = -1.0     # 真实网页加载延迟（百度）
    page_size: int = 0             # 网页响应大小（字节）
    page_ok: bool = False          # 能否正常打开网页
    anonymous: bool = False
    leaks_info: bool = False       # 是否泄露客户端信息（Via/X-Forwarded-For）
    country: str = ""
    city: str = ""
    isp: str = ""
    exit_ip: str = ""
    status: str = "unknown"        # dead / channel_only / alive

    def __str__(self):
        return f"{self.host}:{self.port}"

    @property
    def address(self):
        return f"{self.host}:{self.port}"

    @property
    def full_address(self):
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def country_cn(self):
        return to_cn_country(self.country) or "-"

    @property
    def city_cn(self):
        return to_cn_city(self.city) or "-"

    @property
    def isp_cn(self):
        return to_cn_isp(self.isp) or "-"

    @property
    def location(self):
        """完整归属地（全中文）"""
        parts = [self.country_cn if self.country_cn != "-" else "", self.city_cn if self.city_cn != "-" else ""]
        loc = " ".join(p for p in parts if p)
        isp = self.isp_cn if self.isp_cn != "-" else ""
        if isp:
            loc = f"{loc} ({isp})" if loc else isp
        return loc or "未知"


def check_network() -> bool:
    """启动前检测网络是否可用"""
    log("正在检测网络连接...")
    try:
        req = urllib.request.Request(
            f"http://{TEST_HOST}{TEST_PATH}",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
        log("✅ 网络正常，开始抓取代理\n")
        return True
    except Exception:
        log("❌ 无法连接网络（无法访问测试服务器 api.ip.sb）", "ERROR")
        log("   请检查：1) 网线/Wi-Fi 是否正常  2) 是否需要关闭代理软件后重试")
        return False


# ==================== 日志工具 ====================
def log(msg, level="INFO"):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


# ==================== 抓取模块 ====================
def _download_url(url: str, timeout: int = 12) -> str:
    """下载 URL 内容"""
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _parse_proxy_lines(text: str, default_protocol: str) -> list:
    """解析代理列表文本"""
    proxies = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("<"):
            continue
        m = re.match(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[:\s,]+(\d+)", line)
        if m:
            host, port = m.group(1), int(m.group(2))
            if 1 <= port <= 65535:
                proxies.append(Proxy(host=host, port=port, protocol=default_protocol))
    return proxies


def fetch_proxies_from_url(source: dict) -> list:
    """
    从指定源抓取代理列表。
    GitHub 源会自动尝试多个国内镜像，直到成功为止。
    """
    url = source["url"]
    default_protocol = source["protocol"]

    # 判断是否 GitHub 源，需要镜像
    is_github = "raw.githubusercontent.com" in url or "github.com" in url

    # 构造尝试列表
    if is_github:
        # 先直连，再镜像
        attempt_urls = [url] + [mirror + url for mirror in GITHUB_MIRRORS if mirror]
    else:
        attempt_urls = [url]

    for attempt_url in attempt_urls:
        try:
            text = _download_url(attempt_url, timeout=12)
            proxies = _parse_proxy_lines(text, default_protocol)
            if proxies:
                short_name = "/".join(url.split("/")[-3:-1])
                source_hint = " [镜像]" if attempt_url != url else ""
                log(f"  ✓ {short_name}: {len(proxies)} 个代理{source_hint}")
                return proxies
        except Exception:
            continue

    log(f"  ✗ {url.split('/')[-1]}: 所有线路均失败", "WARN")
    return []


def fetch_all_proxies() -> list:
    log("=" * 60)
    log("步骤 1/4: 从数据源抓取代理（并行 + 镜像加速）")
    log("=" * 60)
    all_proxies = []
    seen = set()
    lock = threading.Lock()

    def _fetch_and_merge(source):
        proxies = fetch_proxies_from_url(source)
        with lock:
            for p in proxies:
                key = (p.protocol, p.host, p.port)  # 去重键含协议
                if key not in seen:
                    seen.add(key)
                    all_proxies.append(p)

    # 并行抓取所有数据源
    with ThreadPoolExecutor(max_workers=min(40, len(PROXY_SOURCES))) as executor:
        futures = [executor.submit(_fetch_and_merge, s) for s in PROXY_SOURCES]
        for f in as_completed(futures):
            f.result()

    log(f"去重后共获得 {len(all_proxies)} 个代理")
    return all_proxies


# ==================== 验证模块 ====================
def _make_http_request() -> bytes:
    return (
        f"GET {TEST_PATH} HTTP/1.1\r\n"
        f"Host: {TEST_HOST}\r\n"
        f"User-Agent: Mozilla/5.0\r\n"
        f"Accept: */*\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()


def _parse_http_response(response: bytes) -> tuple:
    try:
        parts = response.split(b"\r\n\r\n", 1)
        if len(parts) < 2:
            return 0, ""
        header = parts[0].decode("utf-8", errors="ignore")
        body = parts[1].decode("utf-8", errors="ignore").strip()
        status_line = header.split("\r\n")[0]
        if "200" not in status_line:
            return 0, body
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", body):
            return 200, body
        return 0, body
    except Exception:
        return 0, ""


def _test_http_through_socks(sock: socket.socket, timeout: int) -> tuple:
    """通过已建立的 SOCKS 连接发送 HTTP GET 请求到 api.ip.sb/ip"""
    try:
        sock.settimeout(timeout)
        http_req = _make_http_request()
        sock.sendall(http_req)
        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"\r\n\r\n" in response and len(response) > 50:
                try:
                    extra = sock.recv(4096)
                    if extra:
                        response += extra
                except Exception:
                    pass
                break
        status, body = _parse_http_response(response)
        if status == 200 and body:
            return True, body
        return False, ""
    except Exception:
        return False, ""


# ==================== 匿名检测 ====================
_LEAK_HEADERS = ["X-Forwarded-For", "Via", "Proxy-Connection",
                 "X-Real-Ip", "Forwarded", "X-Proxy-Id"]


def _check_headers_for_leaks(headers: dict) -> bool:
    """检查响应头是否泄露了客户端/代理信息"""
    for key in _LEAK_HEADERS:
        if any(key.lower() == h.lower() for h in headers):
            return True
    return False


def _test_anonymity_http(proxy: Proxy, timeout: int) -> bool:
    """通过 HTTP 代理检测是否泄露客户端信息"""
    try:
        proxy_handler = urllib.request.ProxyHandler({
            "http": f"http://{proxy.host}:{proxy.port}",
        })
        opener = urllib.request.build_opener(proxy_handler)
        req = urllib.request.Request(
            "http://httpbin.org/headers",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with opener.open(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
            return _check_headers_for_leaks(data.get("headers", {}))
    except Exception:
        return False


def _test_anonymity_socks(sock_factory, timeout: int) -> bool:
    """通过 SOCKS 代理检测是否泄露客户端信息"""
    sock = None
    try:
        sock = sock_factory()
        sock.settimeout(timeout)
        http_req = (
            b"GET /headers HTTP/1.1\r\n"
            b"Host: httpbin.org\r\n"
            b"User-Agent: Mozilla/5.0\r\n"
            b"Accept: */*\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        )
        sock.sendall(http_req)
        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"\r\n\r\n" in response and len(response) > 300:
                break
        body_start = response.find(b"\r\n\r\n")
        if body_start == -1:
            return False
        body = response[body_start + 4:].decode("utf-8", errors="ignore")
        data = json.loads(body)
        return _check_headers_for_leaks(data.get("headers", {}))
    except Exception:
        return False
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def _test_real_site_through_socks(sock_factory, timeout: int) -> tuple:
    """
    通过代理访问真实网站（百度），测量真实加载延迟
    返回 (是否打开成功, 真实延迟秒, 响应大小字节)
    """
    for site in REAL_TEST_SITES:
        try:
            # 解析目标 IP
            target_ip = socket.gethostbyname(site["host"])
        except Exception:
            continue

        sock = None
        try:
            sock = sock_factory()
            sock.settimeout(timeout)

            # 发送 HTTP GET 请求访问真实网站
            http_req = (
                f"GET {site['path']} HTTP/1.1\r\n"
                f"Host: {site['host']}\r\n"
                f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
                f"Accept: text/html,application/xhtml+xml,*/*\r\n"
                f"Accept-Language: zh-CN,zh;q=0.9\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode()

            start = time.time()
            sock.sendall(http_req)

            # 读取响应（判断能否正常加载网页）
            response = b""
            while True:
                try:
                    chunk = sock.recv(8192)
                    if not chunk:
                        break
                    response += chunk
                    # 收到 HTML 内容就算加载成功
                    if len(response) > 500:
                        break
                except socket.timeout:
                    break

            real_latency = round(time.time() - start, 3)

            # 验证：必须收到 HTTP 200 且有 HTML 内容
            if (b"200 OK" in response or b"200 " in response) and len(response) > 200:
                return True, real_latency, len(response)

        except Exception:
            pass
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    return False, -1, 0


def _real_http_proxy_test(proxy: Proxy, timeout: int) -> tuple:
    """HTTP 代理真实网站测速"""
    for site in REAL_TEST_SITES:
        proxy_handler = urllib.request.ProxyHandler({
            "http": f"http://{proxy.host}:{proxy.port}",
        })
        opener = urllib.request.build_opener(proxy_handler)
        try:
            url = f"http://{site['host']}{site['path']}"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
            start = time.time()
            with opener.open(req, timeout=timeout) as resp:
                data = resp.read()
                real_latency = round(time.time() - start, 3)
                if resp.status == 200 and len(data) > 200:
                    return True, real_latency, len(data)
        except Exception:
            continue
    return False, -1, 0


def verify_http_proxy(proxy: Proxy, timeout: int = 10) -> Proxy:
    """验证 HTTP 代理：出口IP验证 + 真实网站测速 + 匿名检测"""
    proxy_handler = urllib.request.ProxyHandler({
        "http": f"http://{proxy.host}:{proxy.port}",
        "https": f"http://{proxy.host}:{proxy.port}",
    })
    opener = urllib.request.build_opener(proxy_handler)
    start = time.time()
    try:
        url = f"http://{TEST_HOST}{TEST_PATH}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with opener.open(req, timeout=timeout) as resp:
            body = resp.read().decode().strip()
            proxy.latency = round(time.time() - start, 3)
            if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", body):
                proxy.exit_ip = body
            else:
                proxy.status = "dead"
                return proxy
    except Exception:
        proxy.status = "dead"
        proxy.latency = -1.0
        return proxy

    # 第二步：真实网站测速（访问百度）
    ok, real_lat, size = _real_http_proxy_test(proxy, timeout)
    proxy.page_ok = ok
    proxy.real_latency = real_lat
    proxy.page_size = size

    # 第三步：匿名检测
    proxy.leaks_info = _test_anonymity_http(proxy, timeout)

    # 状态判定：只有真能打开网页才算 alive
    if ok:
        proxy.status = "alive"
    else:
        proxy.status = "channel_only"

    return proxy


def verify_socks4_proxy(proxy: Proxy, timeout: int = 10) -> Proxy:
    """验证 SOCKS4 代理：出口IP验证 + 真实网站测速"""
    start = time.time()
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((proxy.host, proxy.port))
        req = struct.pack("!BBH", 0x04, 0x01, 80)
        req += socket.inet_aton(_get_test_ip())
        req += b"\x00"
        sock.sendall(req)
        resp = sock.recv(8)
        if len(resp) < 2 or resp[1] != 0x5A:
            proxy.status = "dead"
            return proxy
        success, exit_ip = _test_http_through_socks(sock, timeout)
        if success:
            proxy.latency = round(time.time() - start, 3)
            proxy.exit_ip = exit_ip
        else:
            proxy.status = "dead"
            return proxy
    except Exception:
        proxy.status = "dead"
        proxy.latency = -1.0
        return proxy
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass

    # 第二步：真实网站测速（通过 SOCKS4 访问百度）
    def _make_sock():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((proxy.host, proxy.port))
        r = struct.pack("!BBH", 0x04, 0x01, 80)
        r += socket.inet_aton(socket.gethostbyname(REAL_TEST_SITES[0]["host"]))
        r += b"\x00"
        s.sendall(r)
        resp = s.recv(8)
        if len(resp) < 2 or resp[1] != 0x5A:
            s.close()
            raise ConnectionError("SOCKS4 握手失败")
        return s

    ok, real_lat, size = _test_real_site_through_socks(_make_sock, timeout)
    proxy.page_ok = ok
    proxy.real_latency = real_lat
    proxy.page_size = size

    # 第三步：匿名检测
    def _make_anon_sock():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((proxy.host, proxy.port))
        r = struct.pack("!BBH", 0x04, 0x01, 80)
        r += socket.inet_aton(socket.gethostbyname("httpbin.org"))
        r += b"\x00"
        s.sendall(r)
        resp = s.recv(8)
        if len(resp) < 2 or resp[1] != 0x5A:
            s.close()
            raise ConnectionError("SOCKS4 握手失败")
        return s

    proxy.leaks_info = _test_anonymity_socks(_make_anon_sock, timeout)

    # 状态判定：只有真能打开网页才算 alive
    proxy.status = "alive" if ok else "channel_only"
    return proxy


def verify_socks5_proxy(proxy: Proxy, timeout: int = 10) -> Proxy:
    """验证 SOCKS5 代理：出口IP验证 + 真实网站测速"""
    start = time.time()
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((proxy.host, proxy.port))
        sock.sendall(b"\x05\x01\x00")
        resp = sock.recv(2)
        if len(resp) < 2 or resp[1] != 0x00:
            proxy.status = "dead"
            return proxy
        req = struct.pack("!BBBB", 0x05, 0x01, 0x00, 0x01)
        req += socket.inet_aton(_get_test_ip())
        req += struct.pack("!H", 80)
        sock.sendall(req)
        resp = sock.recv(10)
        if len(resp) < 2 or resp[1] != 0x00:
            proxy.status = "dead"
            return proxy
        success, exit_ip = _test_http_through_socks(sock, timeout)
        if success:
            proxy.latency = round(time.time() - start, 3)
            proxy.exit_ip = exit_ip
        else:
            proxy.status = "dead"
            return proxy
    except Exception:
        proxy.status = "dead"
        proxy.latency = -1.0
        return proxy
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass

    # 第二步：真实网站测速（通过 SOCKS5 访问百度）
    def _make_sock():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((proxy.host, proxy.port))
        s.sendall(b"\x05\x01\x00")
        resp = s.recv(2)
        if len(resp) < 2 or resp[1] != 0x00:
            s.close()
            raise ConnectionError("SOCKS5 握手失败")
        target_ip = socket.gethostbyname(REAL_TEST_SITES[0]["host"])
        r = struct.pack("!BBBB", 0x05, 0x01, 0x00, 0x01)
        r += socket.inet_aton(target_ip)
        r += struct.pack("!H", 80)
        s.sendall(r)
        resp = s.recv(10)
        if len(resp) < 2 or resp[1] != 0x00:
            s.close()
            raise ConnectionError("SOCKS5 连接失败")
        return s

    ok, real_lat, size = _test_real_site_through_socks(_make_sock, timeout)
    proxy.page_ok = ok
    proxy.real_latency = real_lat
    proxy.page_size = size

    # 第三步：匿名检测
    def _make_anon_sock():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((proxy.host, proxy.port))
        s.sendall(b"\x05\x01\x00")
        resp = s.recv(2)
        if len(resp) < 2 or resp[1] != 0x00:
            s.close()
            raise ConnectionError("SOCKS5 握手失败")
        target_ip = socket.gethostbyname("httpbin.org")
        r = struct.pack("!BBBB", 0x05, 0x01, 0x00, 0x01)
        r += socket.inet_aton(target_ip)
        r += struct.pack("!H", 80)
        s.sendall(r)
        resp = s.recv(10)
        if len(resp) < 2 or resp[1] != 0x00:
            s.close()
            raise ConnectionError("SOCKS5 连接失败")
        return s

    proxy.leaks_info = _test_anonymity_socks(_make_anon_sock, timeout)

    # 状态判定：只有真能打开网页才算 alive
    proxy.status = "alive" if ok else "channel_only"
    return proxy


def verify_proxy(proxy: Proxy, timeout: int = 10) -> Proxy:
    if proxy.protocol == "http":
        return verify_http_proxy(proxy, timeout)
    elif proxy.protocol == "socks4":
        return verify_socks4_proxy(proxy, timeout)
    elif proxy.protocol == "socks5":
        return verify_socks5_proxy(proxy, timeout)
    else:
        proxy.status = "unknown"
        return proxy


def verify_all_proxies(proxies: list, max_workers: int = 80, timeout: int = 3) -> list:
    log("=" * 60)
    log(f"步骤 2/4: 验证代理可用性 (并发: {max_workers}, 超时: {timeout}s)")
    log("=" * 60)
    log("提示：这是最耗时的一步，预计需要 5~15 分钟，可先去忙别的。")
    log("      想中途停止请按 Ctrl+C，已找到的代理会自动保存。\n")

    alive_proxies = []
    completed = 0
    total = len(proxies)
    lock = threading.Lock()
    last_reported = 0
    report_interval = max(1, total // 20)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(verify_proxy, p, timeout): p for p in proxies}
        for future in as_completed(futures):
            proxy = future.result()
            completed += 1
            if proxy.status in ("alive", "channel_only"):
                with lock:
                    alive_proxies.append(proxy)
            if _interrupted:
                log(f"\n已中断，已验证 {completed}/{total}，找到 {len(alive_proxies)} 个可用代理")
                break
            if completed - last_reported >= report_interval or completed == total:
                last_reported = completed
                with lock:
                    pct = completed * 100 // total
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    elapsed = time.time() - start_time
                    if completed > 0 and completed < total:
                        eta_seconds = elapsed / completed * (total - completed)
                        eta_str = f"{int(eta_seconds // 60)}分{int(eta_seconds % 60)}秒"
                    else:
                        eta_str = "计算中"
                    elapsed_str = f"{int(elapsed // 60)}分{int(elapsed % 60)}秒"
                    log(f"  [{bar}] {completed}/{total} ({pct}%)  能用: {len(alive_proxies)}  "
                        f"已用时: {elapsed_str}  预计剩余: {eta_str}")

    log(f"\n验证完成，可用代理: {len(alive_proxies)}/{total}")
    return alive_proxies


# ==================== IP 地理位置查询 ====================
_geo_query_count = 0  # 限流计数器

def query_ip_locations(proxies: list) -> None:
    """使用 api.ip.sb/geoip 查询 IP 归属地（带限流保护）"""
    global _geo_query_count
    log("=" * 60)
    log("步骤 3/4: 查询 IP 地理位置信息 (api.ip.sb)")
    log("=" * 60)

    unique_ips = list(set(p.host for p in proxies))
    location_map = {}
    lock = threading.Lock()
    completed = 0

    def _query_one(ip):
        nonlocal completed
        global _geo_query_count
        # 限流保护：每 20 个请求暂停 1 秒
        with lock:
            _geo_query_count += 1
            if _geo_query_count % 20 == 0:
                time.sleep(1.0)
        info = query_single_ip(ip)
        with lock:
            location_map[ip] = info
            completed += 1
            if completed % 50 == 0 or completed == len(unique_ips):
                log(f"  已查询 {completed}/{len(unique_ips)} 个 IP")

    # 并发查询，api.ip.sb 每个 IP 一个请求（限制并发数防限流）
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(_query_one, unique_ips))

    for p in proxies:
        info = location_map.get(p.host, {})
        p.country = info.get("country", "")
        p.city = info.get("city", "")
        p.isp = info.get("isp", "")

    found = sum(1 for ip in unique_ips if location_map.get(ip, {}).get("country"))
    log(f"IP 地理位置查询完成: {found}/{len(unique_ips)} 个 IP 有归属地信息")


def query_single_ip(ip: str) -> dict:
    """查询单个 IP 归属地，使用 api.ip.sb/geoip"""
    try:
        url = f"https://api.ip.sb/geoip/{ip}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            return {
                "country": data.get("country", "") or "",
                "city": data.get("city", "") or "",
                "isp": data.get("isp", "") or data.get("organization", "") or "",
            }
    except Exception:
        pass
    # 降级到 ip-api.com
    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,country,city,isp"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                return {
                    "country": data.get("country", ""),
                    "city": data.get("city", ""),
                    "isp": data.get("isp", ""),
                }
    except Exception:
        pass
    return {"country": "", "city": "", "isp": ""}


# ==================== 导出模块 ====================
def _open_output(output_file: str, encoding: str):
    """
    打开输出文件；若被占用（Excel 等正打开）则自动改用带时间戳的文件名。
    返回 (file_obj, 实际文件路径)
    """
    candidates = [output_file]
    base, ext = os.path.splitext(output_file)
    ts = time.strftime("%Y%m%d_%H%M%S")
    candidates.append(f"{base}_{ts}{ext}")
    candidates.append(f"{base}_{ts}_{os.getpid()}{ext}")

    for path in candidates:
        try:
            f = open(path, "w", encoding=encoding, newline="")
            return f, path
        except PermissionError:
            log(f"  文件被占用: {path}，尝试其他文件名...", "WARN")
        except OSError as e:
            log(f"  无法写入 {path}: {e}", "WARN")
    raise PermissionError(f"所有候选文件都无法写入: {candidates}")


def export_txt(proxies: list, output_file: str):
    """标准格式：protocol://ip:port，一行一个"""
    f, actual_path = _open_output(output_file, "utf-8")
    with f:
        for p in proxies:
            f.write(f"{p.protocol}://{p.host}:{p.port}\n")
    return actual_path


def export_csv(proxies: list, output_file: str):
    """完整格式 CSV，包含所有筛选字段"""
    f, actual_path = _open_output(output_file, "utf-8-sig")
    with f:
        writer = csv.writer(f)
        writer.writerow([
            "代理地址", "协议", "代理IP", "端口", "出口IP", "IP归属地",
            "国家", "城市", "运营商ISP", "真实延迟(秒)", "通道延迟(秒)",
            "延迟等级", "网页打开", "响应大小", "匿名类型",
            "出口IP验证", "状态", "验证时间",
        ])
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        for p in sorted(proxies, key=lambda x: x.real_latency if x.real_latency > 0 else x.latency):
            real_lat = p.real_latency if p.real_latency > 0 else p.latency
            writer.writerow([
                p.full_address,                          # 代理地址
                PROTOCOL_CN.get(p.protocol, p.protocol), # 协议
                p.host,                                   # 代理IP
                p.port,                                   # 端口
                p.exit_ip or "-",                         # 出口IP
                p.location,                               # IP归属地（全中文）
                p.country_cn,                             # 国家
                p.city_cn,                                # 城市
                p.isp_cn,                                 # 运营商ISP
                f"{real_lat:.3f}" if real_lat > 0 else "",  # 真实延迟（访问百度）
                f"{p.latency:.3f}" if p.latency > 0 else "",# 通道延迟（api.ip.sb）
                get_speed_grade(real_lat),                # 延迟等级
                "正常" if p.page_ok else "失败",           # 能否打开网页
                p.page_size or 0,                         # 响应大小
                get_anonymity_type(p),                    # 匿名类型
                "通过" if (p.exit_ip and p.exit_ip != p.host) else "未变",
                "可用" if p.status == "alive" else "仅通道",  # 真实状态
                now,
            ])
    return actual_path


def print_summary(proxies: list, txt_file: str, csv_file: str):
    log("=" * 60)
    log("步骤 4/4: 导出结果")
    log("=" * 60)

    if not proxies:
        print("\n没有找到存活的代理")
        return

    print(f"\n{'=' * 120}")
    print(f"{'代理地址':<32} {'国家':<10} {'城市':<10} {'真实延迟':<10} {'网页':<6} {'等级':<6} {'匿名':<6}")
    print(f"{'=' * 120}")
    for p in sorted(proxies, key=lambda x: x.real_latency if x.real_latency > 0 else x.latency):
        addr = p.full_address
        country = p.country_cn
        city = p.city_cn[:8]
        real_lat = p.real_latency if p.real_latency > 0 else p.latency
        lat_str = f"{real_lat:.3f}s" if real_lat > 0 else "-"
        page = "正常" if p.page_ok else "失败"
        grade = get_speed_grade(real_lat)
        anon = get_anonymity_type(p)
        print(f"{addr:<32} {country:<10} {city:<10} {lat_str:<10} {page:<6} {grade:<6} {anon:<6}")
    print(f"{'=' * 120}")

    total = len(proxies)
    http_count = sum(1 for p in proxies if p.protocol == "http")
    socks4_count = sum(1 for p in proxies if p.protocol == "socks4")
    socks5_count = sum(1 for p in proxies if p.protocol == "socks5")
    page_ok_count = sum(1 for p in proxies if p.page_ok)
    real_lats = [p.real_latency for p in proxies if p.real_latency > 0]
    avg_real = sum(real_lats) / len(real_lats) if real_lats else 0
    anonymous_count = sum(1 for p in proxies if p.exit_ip and p.exit_ip != p.host)
    fast_count = sum(1 for p in proxies if 0 < (p.real_latency or p.latency) < 1)

    print(f"\n{'=' * 120}")
    print(f"  总计: {total} 个代理")
    print(f"  协议分布: HTTP {http_count} | SOCKS4 {socks4_count} | SOCKS5 {socks5_count}")
    print(f"  平均真实延迟: {avg_real:.3f}s | 极速(<1s): {fast_count} 个")
    print(f"  网页打开成功: {page_ok_count}/{total} | 高匿代理: {anonymous_count}/{total}")
    print(f"{'=' * 120}")
    log(f"TXT 文件: {os.path.abspath(txt_file)}")
    log(f"CSV 文件: {os.path.abspath(csv_file)}")
    log("完成!")


# ==================== 主函数 ====================
def main():
    # 注册 Ctrl+C 优雅中断
    import signal
    signal.signal(signal.SIGINT, _signal_handler)

    parser = argparse.ArgumentParser(
        description="代理服务器抓取与验证工具（一键运行）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--protocol",
        choices=["http", "socks4", "socks5", "all"],
        default="all",
        help="代理协议类型 (默认: all)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="proxies",
        help="输出文件前缀 (默认: proxies)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=80,
        help="并发验证线程数 (默认: 80)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=3,
        help="验证超时时间(秒) (默认: 3)",
    )
    parser.add_argument(
        "--max-latency",
        type=float,
        default=3,
        help="最大延迟过滤(秒), 0=不限制 (默认: 3)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多验证 N 个代理后停止, 0=不限制 (默认: 0)",
    )
    parser.add_argument(
        "--country",
        type=str,
        default="",
        help="按国家筛选（中文或英文），多个用逗号分隔，如：中国,美国",
    )
    parser.add_argument(
        "--anonymous-only",
        action="store_true",
        help="只保留高匿代理",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="只保留最快的 N 个代理, 0=不限制 (默认: 0)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="跳过验证，直接导出所有抓取到的代理",
    )
    parser.add_argument(
        "--no-location",
        action="store_true",
        help="跳过 IP 地理位置查询",
    )

    args = parser.parse_args()

    txt_file = f"{args.output}.txt"
    csv_file = f"{args.output}.csv"

    print()
    log("代理服务器抓取与验证工具")
    print()

    # P1-1: 启动前检测网络
    if not check_network():
        sys.exit(2)

    # 1. 抓取
    proxies = fetch_all_proxies()

    # 2. 按协议过滤
    if args.protocol != "all":
        proxies = [p for p in proxies if p.protocol == args.protocol]
        log(f"过滤后 ({args.protocol}): {len(proxies)} 个代理")

    if not proxies:
        log("没有可验证的代理", "WARN")
        return

    # 2.5 限制验证数量（加快速度）
    if args.limit > 0 and len(proxies) > args.limit:
        proxies = proxies[:args.limit]
        log(f"限制验证数量: {args.limit} 个")

    # 3. 验证
    if args.no_verify:
        alive_proxies = proxies
        log(f"跳过验证，直接使用 {len(alive_proxies)} 个代理")
        # P0-4 修复：未验证时无法按延迟过滤，给出警告
        if args.max_latency > 0:
            log("⚠ 未执行验证，无法按延迟过滤，已忽略 --max-latency 参数", "WARN")
    else:
        alive_proxies = verify_all_proxies(proxies, args.workers, args.timeout)
        # 只保留真能用的（alive）或至少通道通的（channel_only）
        # channel_only 的也被保留但用户能看到网页状态
        channel_only = sum(1 for p in alive_proxies if p.status == "channel_only")
        if channel_only:
            log(f"其中 {channel_only} 个仅通道可用但打不开网页（标记为 channel_only）")

    # 4. 延迟过滤（优先用真实网页延迟）
    if not args.no_verify and args.max_latency > 0:
        def _get_latency(p):
            return p.real_latency if p.real_latency > 0 else p.latency
        before_count = len(alive_proxies)
        alive_proxies = [p for p in alive_proxies if 0 < _get_latency(p) <= args.max_latency]
        log(f"真实延迟过滤后 (<= {args.max_latency}s): {before_count} -> {len(alive_proxies)} 个")

    if not alive_proxies:
        log("没有符合条件的存活代理", "WARN")
        return

    # 5. 查询 IP 地理位置
    if not args.no_location:
        query_ip_locations(alive_proxies)

    # 5.5 按国家筛选
    if args.country:
        countries = [c.strip() for c in args.country.split(",") if c.strip()]
        before_count = len(alive_proxies)
        alive_proxies = [
            p for p in alive_proxies
            if any(p.country_cn == c or p.country == c for c in countries)
        ]
        log(f"按国家筛选后 ({args.country}): {before_count} -> {len(alive_proxies)} 个")

    # 5.6 只保留高匿
    if args.anonymous_only:
        before_count = len(alive_proxies)
        alive_proxies = [p for p in alive_proxies if get_anonymity_type(p) == "高匿"]
        log(f"高匿筛选后: {before_count} -> {len(alive_proxies)} 个")

    # 6. 排序（按真实延迟优先）
    alive_proxies.sort(key=lambda x: x.real_latency if x.real_latency > 0 else x.latency)

    # 6.5 只保留最快的 N 个
    if args.top > 0 and len(alive_proxies) > args.top:
        alive_proxies = alive_proxies[:args.top]
        log(f"只保留最快 {args.top} 个代理")

    # 7. 导出
    actual_txt = export_txt(alive_proxies, txt_file)
    actual_csv = export_csv(alive_proxies, csv_file)

    # 7.5 导出推荐 20 个（最快且能正常打开网页的）
    recommended = [p for p in alive_proxies if p.page_ok and p.status == "alive"]
    recommended = recommended[:20] if len(recommended) > 20 else recommended
    if recommended:
        rec_file = f"{args.output}_top20.txt"
        with open(rec_file, "w", encoding="utf-8") as f:
            f.write("# Top 20 Recommended Proxies (fastest, verified working)\n")
            f.write(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            for p in recommended:
                real_lat = p.real_latency if p.real_latency > 0 else p.latency
                f.write(f"{p.full_address}\n")
        log(f"推荐文件: {os.path.abspath(rec_file)}")

    print_summary(alive_proxies, actual_txt, actual_csv)


if __name__ == "__main__":
    main()
