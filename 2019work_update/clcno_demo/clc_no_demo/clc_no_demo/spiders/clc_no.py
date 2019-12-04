# -*- coding: utf-8 -*-
import scrapy
from clc_no_demo.items import ClcNoDemoItem
import time

class ClcNoSpider(scrapy.Spider):
    name = 'clc_no'
    allowed_domains = ['www.clcindex.com']
    # A、C、D、E、F、G、H、I、J、K、L、M、N、O、P、Q、R、S、T、U、V、W、X、Y、Z
    start_urls = ['http://www.clcindex.com/category/K']

    def parse(self, response):
        index_url = "http://www.clcindex.com"
        clc_nos = response.xpath("//tr[@name='item-row']//td[2]/text()").extract()
        for i,clc_no in enumerate(clc_nos):
            item = ClcNoDemoItem()
            clc_no = clc_no.replace("\t","").replace("\n","")
            clc_name = response.xpath("//tr[@name='item-row']//td[3]//text()").extract()[i].replace("\t","").replace("\n","")
            next_link = index_url +  response.xpath("//tr[@name='item-row']//td[3]/a/@href").extract()[i]
            item['clc_no'] = clc_no
            item['clc_name'] = clc_name
            print(next_link)
            yield scrapy.Request(url = next_link, callback = self.parse)
            yield item
    
            
