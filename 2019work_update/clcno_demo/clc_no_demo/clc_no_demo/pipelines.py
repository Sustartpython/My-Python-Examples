# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pypyodbc
import time

class ClcNoDemoPipeline(object):
    def process_item(self, item, spider):
        return item

class MdbPipeline(object):
    def __init__(self):
        dbpath = r'E:\work\clcno_demo\clc.mdb'
        self.db = pypyodbc.win_connect_mdb(dbpath)

    def process_item(self, item, spider):
        key = item['clc_no']
        value = item['clc_name']
        sql = "insert into data(分类号,分类名称) values ('%s','%s')" % (key,value)
        curser = self.db.cursor()
        curser.execute(sql)
        curser.commit()
        print("%s插入成功%s" % (time.strftime("%Y/%m/%d %X"),key))
        return item