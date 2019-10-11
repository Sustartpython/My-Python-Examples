import requests

url_one = 'https://www.baiten.cn/fgStatus'

cookie = "yunsuo_session_verify=851067b206a30c6c941bc3ff5076f0da; JSESSIONID=732158498DC2675844B80CCAC9778E46; PD=91f9121c41248b4d4a5b3d6368e1c1414f23f48e2f057e4d7f8032471b4d19b640965feac996b590; UM_distinctid=16cd71f37292e7-0d0e753c7137f3-c343162-15f900-16cd71f372a605; CNZZDATA1275904268=38912519-1566974134-%7C1566974134; Hm_lvt_7fc44f078bf7b5e19489428c362109a3=1566977243; Hm_lpvt_7fc44f078bf7b5e19489428c362109a3=1566977606"

Referer = 'https://www.baiten.cn/results/s/%25E4%25BD%259B%25E5%25B1%25B1%25E5%25B8%2582%25E7%25BE%258E%25E7%259A%2584%25E6%25B8%2585%25E6%25B9%2596%25E5%2587%2580%25E6%25B0%25B4%25E8%25AE%25BE%25E5%25A4%2587%25E6%259C%2589%25E9%2599%2590%25E5%2585%25AC%25E5%258F%25B8/.html?type=s'

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'Cookie':cookie,
    'Referer':Referer
}

url_two = 'https://www.baiten.cn/results/list/YTViNGQwMjNmY2ZiNDA0ZDI0ZDExNzI1OWZjNjg1NjI='
#带有session访问
r = requests.session()
res =r.post(url_one,headers)

data = {
    'sc': '',
    'q': '佛山市美的清湖净水设备有限公司',
    'sort': '',
    'sortField': '',
    'fq': '',
    'pageSize': 10,
    'pageIndex': 2,
    'merge': 'no-merge'
}
ress = r.post(url_two,data=data,headers=headers)
print(ress.text)