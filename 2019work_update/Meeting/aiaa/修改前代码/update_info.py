"""
每次更新都下载index静态页面。从index解析出list（插入journal数据库）：
    存在两种情况：1、新的会议；2、旧会议里面新增小会议(几率小，暂时不考虑)
得出每次也要去更新list里面，解析出小会议静态网页（插入issue数据库）
如果小会议静态网页存在detail里，就不做处理，没有的就新增（插入article数据库）
"""

import os
import re
import toml
import math
import time
import redis
import json
import random
import pymysql
import pyhdfs
import requests
import datetime
from queue import Queue
from parsel import Selector
from threading import Thread



class update_info(object):
    def __init__(self):
        r"""
        定义常量
        """
        self.url = 'https://arc.aiaa.org/action/showPublications?pubType=meetingProc&pageSize=100'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        #读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb, decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_aps'))
        # 读取配置文件获取存储路径
        self.indexPath = toml.load('config.toml')['indexPath']
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']
        if not os.path.exists(self.indexPath):
            os.makedirs(self.indexPath)
        if not os.path.exists(self.listPath):
            os.makedirs(self.listPath)
        #创建抓取多线程队列
        self.que = Queue()
        # 创建队列(将执行详情页抓取成功的url放在该队列中，启用一个单线程对数据库更新)
        self.sql_que = Queue()
        #链接数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)

    def update_sql(self,sql):
        r"""
        执行更新语句
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def read_sql(self,sql):
        r"""
        查询语句
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        sql_data = cur.fetchall()
        return sql_data

    def down_index(self):
        r"""
        下载index静态页面
        """
        #先下载第一页，解析出有多少会议，得到具体的页数
        path = self.indexPath + '/0.html'
        count = 0
        while count < 20:
            proxy_one = {
                # 'https':random.choice(self.proxieslist)
            }
            try:
                res_one = requests.get(url=self.url, headers=self.headers, proxies=proxy_one, timeout=20)
                if res_one.status_code == 200:
                    with open(path, mode='w', encoding='utf-8') as f:
                        f.write(res_one.content.decode('utf8'))
                    break
                else:
                    print("%s------ip error and retry!" % path)
                    count +=1
            except:
                print("%s------timeout and retry!" % path)
                count +=1
        with open(path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        #读取大会议的数量，算出页数（100个一页）
        result = html.xpath("//h2[@class='search-result__title']/text()").getall()[0]
        page = re.findall("\d+", result)[0]
        totalpage = math.ceil(int(page) / 100)
        for page in range(0,totalpage):
            fname = self.indexPath + '/%s.html' % page
            url = "https://arc.aiaa.org/action/showPublications?pubType=meetingProc&startPage=%s&pageSize=100" % page
            #测试代码，正式代码中注释掉，保证每次重新下载index文件
            # if os.path.exists(fname):
            #     print("已存在")
            #     self.down_list(fname)
            #     continue
            while True:
                proxy_two = {
                    'https': random.choice(self.proxieslist)
                }
                try:
                    res_two = requests.get(url, headers=self.headers, proxies=proxy_two, timeout=20)
                    if res_two.status_code == 200:
                        with open(fname, mode='w', encoding='utf-8') as fw:
                            fw.write(res_two.content.decode('utf8'))
                        print("%s down right!" % urlx)
                        self.down_list(fname)
                    else:
                        print("%s------ip error and retry!" % urlx)
                        break
                except:
                    print("%s------timeout and retry!" % urlx)
                    break

    def down_list(self,fname):
        r"""
        下载大会议含有多少小会议list静态网页
        """
        with open(fname, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        doi = html.xpath("//h4[@class='search-item__title']/a/@href").getall()
        titles = html.xpath("//h4[@class='search-item__title']/a/text()").getall()
        print('该网页有%s个网页链接' % len(doi))
        for i,item in enumerate(doi):
            try:
                name = re.findall('[A-Z].*', item)[0]
            except:
                name = re.findall('10.2514/([a-z].*)', item)[0]
            list_url = "https://arc.aiaa.org" + item
            title = titles[i]
            sql = "insert ignore into lists(url,name,title) VALUES (%s,%s,%s)"
            message = (list_url,name,title)
            cur = self.conn.cursor()
            cur.execute(sql,message)
            self.conn.commit()
            continue

        while True:
            proxy = {
            'https': random.choice(self.proxieslist)
        }
            sql = "select url,name from lists where stat = 0"
            urls = self.read_sql(sql)
            if urls == ():
                print("暂时无需更新！！！")
                break
            else:
                for urlxx in urls:
                    # try:
                    urlx = urlxx[0]
                    name = urlxx[1]
                    list_fname = self.listPath + '/%s.html' % name
                    print(urlx)
                    # if os.path.exists(list_fname):
                    #     print('该会议已抓取')
                    #     continue
                    # else:
                    try:
                        res = requests.get(urlx, headers=self.headers, proxies=proxy, timeout=120)
                        if res.status_code == 200:
                            with open(list_fname, mode='w', encoding='utf-8') as f:
                                f.write(res.content.decode('utf8'))
                            print("%s下载成功!" % urlx)
                            html = Selector(text, type='html')
                            sql = "update lists set stat = 1 where url = '%s'" %(urlx)
                            self.update_sql(sql)
                            self.get_url(list_fname)
                        else:
                            print("%s------ip error and retry!" % urlx)
                            break
                    except Exception as e:
                        print(e)
                        print("%s------timeout and retry!" % urlx)
                        break
        
    def get_url(self,list_fname):
        r"""
        解析list列表页面，将解析出的数据存入mysql
        """
        with open(list_fname, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        result = html.xpath("//h5[@class='issue-item__title']/a/@href").getall()
        # time.sleep(120)
        titles =  html.xpath("//h5[@class='issue-item__title']/a/text()").getall()
        eISBN = html.xpath("//div[@class='teaser__row'][2]/div[@class='teaser__item']/text()").getall()
        if eISBN != []:
            if "eISBN" in eISBN[0]:
                eisbn = eISBN[0].replace("eISBN:","").replace("-","")
        else:
            eisbn = ""
        if result is None:
            return
        else:
            for i, item in enumerate(result):
                lists = []
                self.i +=1
                url = 'https://arc.aiaa.org' + item
                name = re.findall('10.2514/(.*)', item)[0]
                title = titles[i]
                try:
                    session = html.xpath("//div[@class='issue-item__session_details']/text()").getall()[i]
                except:
                    session = ""
                message = (url,name,title,session,eisbn)
                sql = "insert ignore into detail (url,doi,title,session,eisbn) values(%s,%s,%s,%s,%s)"
                cur = self.conn.cursor()
                cur.execute(sql,message)
                self.conn.commit()
                # sql = "update detail set eisbn = %s where url = %s"
                # message = (eisbn,url)
                # cur = self.conn.cursor()
                # cur.execute(sql,message)
                # self.conn.commit()


        
    def get_date(self):
        r"""
        获取详情页信息
        """
        now_time = datetime.datetime.now().strftime("%Y%m%d")
        path = r'E:\work\Meeting\aiaa\big_json'
        os.chdir(path)
        pwd = os.getcwd()+"\\"+now_time
        # 文件路径
        big_json_path = os.path.exists(pwd)
        # 判断文件是否存在：不存在创建
        if not big_json_path:
            os.makedirs(pwd)
        big_json_name =pwd + '/%s.big_json' % (now_time)
        fw = open(big_json_name, 'a', encoding='utf-8')
        while True:
            proxy = {
                'https': random.choice(self.proxieslist)
            }
            # 获取URL
            sql = "select url,session,eisbn from detail where stat = 0"
            urls = self.read_sql(sql)
            print("此次更新的数据为%s条"%len(urls))
            if urls == ():
                print("暂时无需更新！！！")
                break
            else:
                for urlxx in urls:
                    # try:
                    url = urlxx[0]
                    session = urlxx[1]
                    eisbn = urlxx[2]
                    try:
                        res = requests.get(url=url, headers=self.headers,proxies=proxy,timeout=60)
                        if res.status_code == 200:
                            text_one = res.content.decode('utf8').strip()
                            sumDict = dict()
                            sumDict['url'] = url
                            sumDict['session'] = session
                            sumDict['eisbn'] = eisbn
                            sumDict['down_date'] = now_time
                            sumDict['htmlText'] = text_one
                            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                            fw.write(line)
                            print('%s写入成功'%url)
                            sql = "update detail set stat = 1 where url = '%s'" % (url)
                            self.update_sql(sql)

                        else:
                            print("%s------ip error and retry!" % url)
                            break
                    except Exception as e:
                        print(e)
                        print("%s------timeout and retry!" % url)
                        break

    def up_hdfs(self):
        r"""
        上传到hadoop上
        """
        #链接hdfs
        client = pyhdfs.HdfsClient(hosts='hadoop2x-01:50070,hadoop2x-02:50070',user_name='suh')
        now_time = datetime.datetime.now().strftime("%Y%m%d")
        year = time.strftime('%Y',time.localtime(time.time()))
        HdfsDir = r'/RawData/aiaa_meeting/big_json/%s/%s'%(year,now_time)
        if not client.exists(HdfsDir):
            client.mkdirs(HdfsDir)
        print('Before !%s' % client.listdir(HdfsDir))
        #从本地上传文件至集群
        local_path = r'E:\work\Meeting\aiaa\big_json\%s\%s.big_json' % (now_time,now_time)
        up_path = HdfsDir + '/%s.big_json' % (now_time)
        if client.exists(up_path):
            client.delete(up_path)
        client.copy_from_local(local_path,up_path)
        print('After !%s' % client.listdir(HdfsDir))


if __name__ == "__main__":
    news = update_info()
    news.down_index()
    news.get_date()
    news.up_hdfs()

                





        
        
