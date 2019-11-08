import requests,json
from parsel import Selector
from PIL import Image

url = "http://mirror.lib.swjtu.edu.cn:90/?app=organization&controller=import&action=unpack&bookid=1112"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
    }
proxy = {
        'http':'192.168.30.176:8131'
    }

res = requests.get(url,headers=headers,proxies=proxy)
text = res.text
data = json.loads(text)
print(len(data["data"]))
a = ""
for i,item in enumerate(data['data']):
    # print(item)
    title = item['title']
    try:
        nodes = item['node']
        jie = ""
        for n,node in enumerate(nodes):
            jie += node['title'] + ";" 
        a += title + ":" + jie + '\n'
        a = a.replace("'","\"")
    except:
        a += title + '\n'
        a = a.replace("'","\"")
        
    

print(a)
    
# html = Selector(text, type='html')
# provider_subjects = html.xpath("//title/text()").extract_first("").split(" - ")[1:3]
# provider_subject = ""
# for i,item in enumerate(provider_subjects):
#     provider_subject += item + ';'
# print(provider_subject)

# title = html.xpath("//h2[@class='book_title']/text()").extract_first("")
# creator = html.xpath("//li[@class='book_detail_author']/a/text()").extract_first("").replace(",",";")
# date_created = html.xpath("//li[@class='book_detail_time']/text()").extract_first("19000000").replace("出版日期：","").replace(".","") + "00"
# date = date_created[:4]

# publisher = html.xpath("//li[@class='book_detail_press']/text()").extract_first("").replace("出版社：","")
# identifier_eisbn = html.xpath("//li[@class='book_detail_isbn']/text()").extract_first("").replace("ISBN：","").replace("-","")
# description = html.xpath("//div[@class='desc']/text()").extract_first("")

# urls = html.xpath("//a[@class='main_lib_book_a']/@href").getall()
# for i, item in enumerate(urls):
#     link = "http://mirror.lib.swjtu.edu.cn:90/" + item
#     image_url = html.xpath("//a[@class='main_lib_book_a']/img/@src").getall()[i]
#     title =  html.xpath("//a[@class='main_lib_book_a']/img/@alt").getall()[i]


# import re
# url = "http://mirror.lib.swjtu.edu.cn:90/?app=book&controller=index&action=show&bookid=1"

# bookid = re.findall('bookid=(.*)',url)[0]
# print(bookid)
