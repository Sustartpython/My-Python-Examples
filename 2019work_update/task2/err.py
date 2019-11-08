# from configparser import ConfigParser
# config = ConfigParser()
# with open("config.ini","r") as cfgfile:
#      config.read_file(cfgfile)
#      DBHOST = config.get( "fileroot", "Dbpath" )
# print(DBHOST)


import requests
from lxml import etree
from parsel import Selector

res = requests.get("https://ascelibrary.org/ebooks?pageSize=20&startPage=0")
res.encoding = res.apparent_encoding
html = Selector(res.text,'html')
year = html.xpath("//div[@class='listing-view']/div[@class='listBody']/div[@class='listItem odd clearfix' or @class='listItem even clearfix'][1]/div[@class='rightSide']/div[@class='itemFooter']/span[@class='year']/text()").getall()
print(year)
print(len(year))