import requests

url = 'http://www.springer.com'
headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            "Accept":"*/*",
        }
proxy = {
    "HTTP":"192.168.30.172:9172",
        }
res = requests.get(url,headers=headers,proxies=proxy)
res.encoding = 'utf-8'
print(res.text)