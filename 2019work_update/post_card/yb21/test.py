import requests

url = 'http://www.yb21.cn/post/city/5731.html'

res = requests.get(url)
print(res.status_code)
print(res.apparent_encoding)
with open('1.html','w',encoding='GB2312')as f:
    f.write(res.content.decode('GB2312'))