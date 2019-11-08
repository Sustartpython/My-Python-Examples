import requests
from parsel import Selector
url = "http://muchong.com/bbs/journal_cn.php?view=detail&jid=149"

res = requests.get(url)
text = res.text

html = Selector(text,'html')

table = html.xpath("//div[@class='forum_explan bg_global']//dl//table//tr")
for tr in table.xpath('string(.)').extract():
    print(tr.replace('\n','').replace('\xa0',';'))