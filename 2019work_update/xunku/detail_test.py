url = 'http://app.xunku.org/modules/epaper/content.jsp?orgurl=http://xh.xhby.net/newxh/html/2010-07/09/content_253036.htm&hashcode=2066059378&url=/xh_xhby_net/newxh/html/2010-07/09/content_253036.htm&sourceID=100031&searchtype=0&date=20100709'

import requests
from parsel import Selector


headers_2 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        'Rerfer':'http://app.xunku.org',
        'Cookie':'JSESSIONID=AF4F19EB52E427DB962025DFA8C062CA; DWRSESSIONID=GkwiWxPEhZKmGLsW2h1rukEDbPm'
    }

res =requests.get(url,headers=headers_2)
text = res.content.decode('utf8').strip()
html = Selector(text,type='html')
text_titles = html.xpath("//h2[@align='center']/text()").getall()
content = html.xpath("//div[@class='content']/text()").getall()
contents = ""
for m in content:
    x = m.strip()
    contents += x
print(contents)
print("--------------------------")
