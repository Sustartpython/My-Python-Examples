import os
import re
import toml
import time
import redis
import random
import requests
import pypyodbc
import pymysql
import datetime
from queue import Queue
from parsel import Selector

class mc(object):
    def __init__(self):
        self.headers = {
            "Accept" : "*/*",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
        }
        # 读取配置文件获取mdb数据库地址,链接数据库
        self.DBPATH = toml.load('config.toml')['Dbpath']
        self.db = pypyodbc.win_connect_mdb(self.DBPATH)
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb, decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_acs'))
        # 读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        # 链接mysql数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        # 创建详情url的队列
        self.detail_url_que = Queue()

    
    def down_detail(self):
        for jid in range(1,1078):
            url = "http://muchong.com/bbs/journal_cn.php?view=detail&jid=%s" % jid
            self.detail_url_que.put(url)
        while True:
            if not self.detail_url_que.empty():
                proxy = {
                    'https': random.choice(self.proxieslist)
                }
                url = self.detail_url_que.get()
                try:
                    res = requests.get(url,headers=self.headers,proxies=proxy)
                    if res.status_code == 200:
                        res.encoding = res.apparent_encoding
                        html = Selector(res.text,'html')
                        table = html.xpath("//div[@class='forum_explan bg_global']//dl//table//tr")
                        for tr in table.xpath('string(.)').extract():
                            tr = tr.replace('\n','').replace('\xa0',';').replace("'",'').strip()
                            # 期刊名：
                            if tr.startswith('期刊名：'):
                                journal_name = tr.replace('期刊名：','').replace('我要投此期刊','')
                            # 期刊英文名：
                            if tr.startswith('期刊英文名：'):
                                journal_name_en = tr.replace('期刊英文名：','')
                            # 出版周期：
                            if tr.startswith('出版周期：'):
                                journal_cycle = tr.replace('出版周期：','')
                            # 出版ISSN：
                            if tr.startswith('出版ISSN：'):
                                issn = tr.replace('出版ISSN：','')
                            # 出版CN：
                            if tr.startswith('出版CN：'):
                                cn = tr.replace('出版CN：','')
                            # 邮发代号：
                            if tr.startswith('邮发代号：'):
                                post_code = tr.replace('邮发代号：','')
                            # 主办单位：
                            if tr.startswith('主办单位：'):
                                publishplace = tr.replace('主办单位：','')
                            # 出版地：
                            if tr.startswith('出版地：'):
                                place = tr.replace('出版地：','')
                            # 期刊主页网址：
                            if tr.startswith('期刊主页网址：'):
                                journal_url = tr.replace('期刊主页网址：','')
                            # 数据库收录/荣誉：
                            if tr.startswith('数据库收录/荣誉：'):
                                journal_honors = tr.replace('数据库收录/荣誉：','')
                            # 复合影响因子：
                            if tr.startswith('复合影响因子：'):
                                double_reason = tr.replace('复合影响因子：','')
                            # 综合影响因子：
                            if tr.startswith('综合影响因子：'):
                                all_reason = tr.replace('综合影响因子：','')
                            # 偏重的研究方向：
                            if tr.startswith('偏重的研究方向：'):
                                researchdirection = tr.replace('偏重的研究方向：','').replace('-','')
                            # 投稿录用比例：
                            if tr.startswith('投稿录用比例：'):
                                ratio = tr.replace('投稿录用比例：','').replace('-','')
                            # 审稿速度：
                            if tr.startswith('审稿速度：'):
                                speed = tr.replace('审稿速度：','').replace('-','')
                            # 审稿费用：
                            if tr.startswith('审稿费用：'):
                                review_money = tr.replace('审稿费用：','').replace('-','')
                            # 版面费用：
                            if tr.startswith('版面费用：'):
                                money = tr.replace('版面费用：','').replace('-','')
                        # 存入 mysql 保留信息
                        sql_1 = "insert ignore into muchong (url,journal_name) values ('%s','%s')" % (url,journal_name)
                        cur = self.conn.cursor()
                        cur.execute(sql_1)
                        self.conn.commit()
                        # 存入 mdb 信息
                        sql_2 = "insert into data(url,期刊名,期刊英文名,出版周期,出版ISSN,出版CN,邮发代号,主办单位,出版地,期刊主页网址,数据库收录荣誉,复合影响因子,综合影响因子,偏重的研究方向,投稿命中率,审稿周期,审稿费,版面费) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (url,journal_name,journal_name_en,journal_cycle,issn,cn,post_code,publishplace,place,journal_url,journal_honors,double_reason,all_reason,researchdirection,ratio,speed,review_money,money)
                        curser = self.db.cursor()
                        curser.execute(sql_2)
                        curser.commit()
                        print('%s插入成功'%journal_name)
                    else:      
                        self.detail_url_que.put(url)
                        print('%s重新下载' % url)
                except Exception as e:
                    print(url)
                    print(e)
                    time.sleep(999)
            else:
                break

if __name__ == "__main__":
    add_ = mc()
    add_.down_detail()
