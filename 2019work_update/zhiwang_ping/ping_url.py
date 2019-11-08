import requests
import time
import sqlite3

proxy_1 = {
    'http':'192.168.30.3:8080'
}

headers = {
        "Accept" : "*/*",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    }

def isRightKNS():
    while True:
        conn = sqlite3.connect("cnkijournal_2019-10-22_69.db3")
        sql = "select provider_url,rawid from modify_title_info_zt limit 1000"
        cur = conn.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for provider_url,rawid in rows:
                url = provider_url.replace("cnkijournal@",'')
                try:
                    print(url)
                    res = requests.get(url,proxies=proxy_1,headers=headers)
                    print(res.status_code)
                except Exception as e:
                    print(e)




if __name__ == "__main__":
    isRightKNS()