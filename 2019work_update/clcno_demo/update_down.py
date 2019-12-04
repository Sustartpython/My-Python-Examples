from parsel import Selector
import time
import requests
import utils
import redis
import pypyodbc
import sqlite3
import random


dbpath = r'E:\work\clcno_demo\data1.db3'
mdbpath = r'E:\work\clcno_demo\中图分类号_20191202.mdb'
index_html = r'E:\work\clcno_demo\html\index.html'
conn = sqlite3.connect(dbpath)
db = pypyodbc.win_connect_mdb(mdbpath)

index_url = "http://www.clcindex.com"

def parse_index():
    with open(index_html,encoding='utf-8')as f:
        text = f.read()
    html = Selector(text,'html')
    clc_nos = html.xpath("//tr[@name='item-row']//td[2]/text()").extract()
    for i,clc_no in enumerate(clc_nos):
        clc_no = clc_no.replace("\t","").replace("\n","")
        clc_name = html.xpath("//tr[@name='item-row']//td[3]//text()").extract()[i].replace("\t","").replace("\n","")
        sql = "insert or replace into clc(fcode,info) values ('%s','%s')" % (clc_no,clc_name)
        curser = conn.cursor()
        curser.execute(sql)
        conn.commit()
        utils.printf("%s 插入成功" % clc_no)

def parse(fcode):
    url = "http://www.clcindex.com/category/%s/" % fcode
    ip = {"http":random_ip()}
    try:
        res = requests.get(url,proxies=ip,timeout=3)
        if res.status_code == 200:
            print(url)
            print(ip)
            html = Selector(res.text,'html')
            clc_nos = html.xpath("//tr[@name='item-row']//td[2]/text()").extract()
            for i,clc_no in enumerate(clc_nos):
                clc_no = clc_no.replace("\t","").replace("\n","")
                clc_name = html.xpath("//tr[@name='item-row']//td[3]//text()").extract()[i].replace("\t","").replace("\n","")
                sql = "insert or replace into clc(fcode,info) values ('%s','%s')" % (clc_no,clc_name)
                curser = conn.cursor()
                curser.execute(sql)
                conn.commit()
                utils.printf("%s 插入成功" % clc_no)
            sql_up = "update clc set stat = 1 where fcode = '%s'" % fcode
            curser = conn.cursor()
            curser.execute(sql_up)
            conn.commit()
        else:
            print("ip err")
    except Exception as e:
        print(e)

def mysql_2_mdb():
    # 查询mysql数据
    cur = conn.cursor()
    sql = "select fcode,info from clc"
    cur.execute(sql)
    rows = cur.fetchall()
    for fcode,info in rows:
        fcode = fcode.replace('[','').replace(']','')
        # 链接mdb数据库插入数据
        sql_in = "insert into data(分类号,分类名称) values ('%s','%s')" % (fcode,info)
        curser = db.cursor()
        curser.execute(sql_in)
        curser.commit()
        print('%s插入成功' % fcode)

def random_ip():
    redisHost = r'192.168.30.36'
    redisPort = r'6379'
    redisDb = r'0'
    connRedis = redis.StrictRedis(host=redisHost,
                                  port=redisPort,
                                  db=redisDb,
                                  decode_responses=True)
    RKEY_PROXY = connRedis.keys()
    RKEY_PROXY.remove("update_time")
    key = random.choice(RKEY_PROXY)
    proxy = connRedis.srandmember(key)
    return proxy

if __name__ == "__main__":
    # parse_index()
    # while True:
    #     cur = conn.cursor()
    #     # 查询数据库 把stat = 0 全部取出来 调用parse函数
    #     sql = "select fcode,info from clc where stat = 0"
    #     cur.execute(sql)
    #     rows = cur.fetchall()
    #     if len(rows) == 0:
    #         break
    #     else:
    #         for fcode,_ in rows:
    #             parse(fcode)
    mysql_2_mdb()
    
