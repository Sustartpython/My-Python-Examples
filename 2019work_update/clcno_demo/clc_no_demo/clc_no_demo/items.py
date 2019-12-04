# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ClcNoDemoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    clc_no = scrapy.Field()
    clc_name = scrapy.Field()
    print(clc_no,clc_name)
