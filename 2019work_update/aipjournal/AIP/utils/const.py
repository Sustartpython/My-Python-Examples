"""
文件路径
"""
import os
# 目录(src)
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# utils 目录
UTILS_PATH = os.path.abspath(os.path.dirname(__file__))
# 资源文件总目录
FILE_DOWNLOAD_PATH = os.path.join(os.path.abspath(os.path.join(os.path.dirname(SRC_PATH), '.', 'download')))

HEADER = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
    'cache-control':
        "no-cache",
    "Connection":
        "keep-alive",}

LANGUAGE = {
    "英文": "EN",
    "阿拉伯文": "AR",
    "白俄罗斯文": "BE",
    "保加利亚文": "BG",
    "加泰罗尼亚文": "CA",
    "捷克文": "CS",
    "丹麦文": "DA",
    "德文": "DE",
    "希腊文": "EL",
    "西班牙文": "ES",
    "爱沙尼亚文": "ET",
    "芬兰文": "FI",
    "法文": "FR",
    "克罗地亚文": "HR",
    "匈牙利文": "HU",
    "冰岛文": "IS",
    "意大利文": "IT",
    "希伯来文": "IW",
    "日文": "JA",
    "朝鲜文": "KO",
    "立陶宛文": "LT",
    "拉托维亚文(列托)": "LV",
    "马其顿文": "MK",
    "荷兰文": "NL",
    "挪威文": "NO",
    "波兰文": "PL",
    "葡萄牙文": "PT",
    "罗马尼亚文": "RO",
    "俄文": "RU",
    "塞波尼斯-克罗地亚文": "SH",
    "斯洛伐克文": "SK",
    "斯洛文尼亚文": "SL",
    "阿尔巴尼亚文": "SQ",
    "塞尔维亚文": "SR",
    "瑞典文": "SV",
    "泰文": "TH",
    "土耳其文": "TR",
    "乌克兰文": "UK",
    "中文": "ZH",
    "国际": "UN",
    "繁体中文": "ZH",
    '繁體中文': "ZH",
    "简体中文": "ZH",
    "其他":"UN"}