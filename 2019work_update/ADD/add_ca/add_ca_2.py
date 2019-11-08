import os
import re
import toml
import time
import json
import redis
import random
import requests
import pypyodbc
import pymysql
import datetime
from queue import Queue
from parsel import Selector


class add_ca_2(object):
    def __init__(self):
        self.url = "https://www.bigan.net/contribute/ajax.aspx?oper=ajaxSearchJournal"
        self.headers = {
            "Accept" : "*/*",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
            'Referer':'https://www.bigan.net/contribute/contribute_list.aspx',
        }
        # 读取配置文件获取list网页存放路径
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        self.list_path = toml.load('config.toml')['list_path_2']
        self.list_path = self.list_path + "\\" + self.now_time
        if not os.path.exists(self.list_path):
            os.makedirs(self.list_path)
        # 读取配置文件获取mdb数据库地址,链接数据库
        self.DBPATH = toml.load('config.toml')['Dbpath_two']
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
        # 创建队列，将解析出来的详情页url存入
        self.detail_url_que = Queue()
        # 创建队列，将list页的页数存入
        self.list_num_que = Queue()
        self.count = 0

    def down_list(self):
        r"""
        解析列表页，ajax，post请求，202页
        """
        for i in range(1,39):
            self.list_num_que.put(i)
        while True:
            proxy = {
                # 'https': random.choice(self.proxieslist)
            }
            if not self.list_num_que.empty():
                page = self.list_num_que.get()
                data = {
                    'data': '{"page":%s,"pagesize":10000,"keyword":"","minfactor":"","maxfactor":"","subject_id":"0","journal_type":[]}' % str(page)
                }
                path = self.list_path + '/%s.html' % page
                if os.path.exists(path):
                    print("第%s页存在"% page)
                    self.get_detail_url(path)
                else:
                    try:
                        res = requests.post(self.url,headers=self.headers,data=data,proxies=proxy)
                        if res.status_code != 200:
                            print("%s页重新添加到队列" % page)
                            self.list_num_que.put(page)
                            continue
                        else:
                            with open(path, mode='a', encoding='utf-8')as f:
                                f.write(res.content.decode('utf8'))
                            print("第%s页缓存成功" % page)
                            # self.get_detail_url(path)
                    except Exception as e:
                        print(e)
                        print("%s页重新添加到队列" % page)
                        self.list_num_que.put(page)
                        continue
            else:
                break

    def get_detail_url(self,path):
        with open(path, encoding='utf8') as f:
            text = f.read()
        info = json.loads(text)['table']
        try:
            for message in info:
                self.count +=1
                # 期刊名称
                title = message['name'].replace("'","^")
                _id = message['id']
                url = "https://www.bigan.net/contribute/contribute_details.aspx?id=%s" % _id
                sql = "insert ignore into add_ca_two (url,journal_name) values ('%s','%s')"%(url,title)
                cur = self.conn.cursor()
                cur.execute(sql)
                self.conn.commit()
                print("该页链接插入mysql完毕")
                # 期刊简介
                abstract = message['abstract'].replace("'","^")
                # 期刊ISSN
                issn = message['issn']
                # 最新影响因子
                impactfactor = message['impactfactor']
                # 出版国家或地区
                publishplace = message['publishplace'].replace("'","^")
                # 出版周期
                publishcycle = message['publishcycle'].replace("'","^")
                if publishcycle == '':
                    publishcycle = '不详'
                # 年文章数
                articlecount = message['articlecount'].replace("'","^")
                if articlecount == '0':
                    articlecount = '正在统计'
                # 出版年份
                year = message['year'].replace("'","^")
                if year == '0':
                    year = '正在收集'
                # 发布状态
                status = message['status'].replace("'","^")
                # 是否OA开放访问
                openaccess = message['openaccess']
                if openaccess == 'False':
                    openaccess = '否'
                else:
                    openaccess = '是'
                # 涉及的研究方向
                researchdirection = message['researchdirection'].replace("'","^").replace("-","")
                # 是否SCI
                sci = message['sci']
                if sci == 'False':
                    sci = '否'
                else:
                    sci = '是'
                # 是否EI
                ei = message['ei']
                if ei == 'False':
                    ei = '否'
                else:
                    ei = '是'
                # 是否CA
                ca = message['ca']
                if ca == 'False':
                    ca = '否'
                else:
                    ca = '是'
                # 是否中文核心期刊
                core = message['core']
                if core == 'False':
                    core = '否'
                else:
                    core = '是'
                # 通讯方式
                communication = message['communication']
                # 期刊官网网站
                officialwebsite = message['officialwebsite'].replace("'","^")
                # 影响因子趋势
                go = ''
                # 平均录用比例
                difficulty = message['difficulty']
                # 平均审稿速度
                auditspeed = message['auditspeed']
                if auditspeed == '':
                    auditspeed = '不详'
                sql = "insert into data (url,期刊名称,期刊简介,期刊ISSN,最新影响因子,出版国家或地区,出版周期,年文章数,出版年份,发布状态,是否OA开放访问,涉及的研究方向,是否SCI,是否EI,是否CA,是否中文核心期刊,通讯方式,期刊官方网站,影响因子趋势,平均录用比例,平均审稿速度) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (url,title,abstract,issn,impactfactor,publishplace,publishcycle,articlecount,year,status,openaccess,researchdirection,sci,ei,ca,core,communication,officialwebsite,go,difficulty,auditspeed)
                curser = self.db.cursor()
                curser.execute(sql)
                curser.commit()
                print("第%s条,%s插入成功" % (self.count,title))
        except Exception as e:
            print(url)
            print(e)
            time.sleep(999)
            

if __name__ == "__main__":
    ca = add_ca_2()
    ca.down_list()
