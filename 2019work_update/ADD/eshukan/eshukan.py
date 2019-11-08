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

class eshukan(object):
    def __init__(self):
        self.headers = {
            "Accept" : "*/*",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
        }
         # 读取配置文件获取list网页存放路径
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        self.list_path = toml.load('config.toml')['list_path']
        self.detail_path = toml.load('config.toml')['detail_path']
        # self.list_path = self.list_path + "\\" + self.now_time
        self.list_path = self.list_path + "\\" + '20190925'
        # self.detail_path = self.detail_path + "\\" + self.now_time
        self.detail_path = self.detail_path + "\\" + '20190924'
        if not os.path.exists(self.list_path):
            os.makedirs(self.list_path)
        if not os.path.exists(self.detail_path):
            os.makedirs(self.detail_path)
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
        # 创建队列，存入列表页url
        self.list_url_que = Queue()
        # 创建队列，存入详情页url
        self.detail_url_que = Queue()

    def down_list(self):
        for i in range(1,1597):
            self.list_url_que.put(i)
        while True:
            if not self.list_url_que.empty():
                proxy = {
                    'https': random.choice(self.proxieslist)
                }
                num = self.list_url_que.get()
                list_url = "http://www.eshukan.com/SuperSearchList.aspx?keyword=&classify=0&wenZhong=-1&kanQi=0&area=0&level=0&heXin=0&puKan=0&first=0&countrySupport=0&college=0&yxyz=0&hornor=0&contentIncluded=0&doubleAnonymous=0&comment=0&gaoFei=0&banMianFei=0&banMianFeiArea=0&shenGaoTime=-1&hot=-1&method=0&page=%s" % str(num)
                path = self.list_path + '/%s.html' % num
                if os.path.exists(path):
                    print("存在")
                    self.down_detail(path)
                else:
                    try:
                        res = requests.get(list_url,headers=self.headers,proxies=proxy)
                        if res.status_code != 200:
                            print("%s页重新添加到队列" % num)
                            self.list_url_que.put(num)
                            continue
                        else:
                            with open(path, mode='a', encoding='utf-8')as f:
                                f.write(res.content.decode('utf8'))
                            print("第%s页缓存成功"%num)
                            self.down_detail(path)
                    except Exception as e:
                        print("错误1 %s " % e)
                        self.list_url_que.put(num)
                        time.sleep(999)
            else:
                break
    
    def down_detail(self,path):
        with open(path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        link = html.xpath("//div[@class='jname']/a/@href").extract()
        title = html.xpath("//div[@class='jname']/a/text()").extract()
        for i,item in enumerate(link):
            journal_name = title[i].replace("\r\n","").strip().replace("'","^")
            url = "http://www.eshukan.com" + item
            sql = "insert ignore into eshukan (url,journal_name) values ('%s','%s')"%(url,journal_name)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            self.detail_url_que.put(url)
            print("%s插入队列成功" % url)
        print("详情页插入mysql完毕")
        while True:
            if not self.detail_url_que.empty():
                proxy = {
                    'https': random.choice(self.proxieslist)
                }
                detail_url = self.detail_url_que.get()
                jid = re.findall('jid=(\d+)', detail_url)[0]
                detailpath = self.detail_path + '/%s.html' % jid
                if os.path.exists(detailpath):
                    print('%s已存在'%detailpath)
                    # self.write_2_mdb(detailpath,detail_url)
                else:
                    try:                  
                        res = requests.get(detail_url,headers=self.headers,proxies=proxy,timeout=30)
                        if res.status_code == 200:
                            res.encoding = res.apparent_encoding
                            text = res.text
                            if '<div class="wrong">' in text:
                                print("%s网页错误" % detail_url)
                            else:
                                with open(detailpath, mode='a', encoding='utf-8')as f:
                                    f.write(res.content.decode('utf8'))
                                print("jid = %s 缓存成功" % jid)
                                # self.write_2_mdb(detailpath,detail_url)
                        else:
                            self.detail_url_que.put(detail_url)
                            print("%s重新加入到队列" % detail_url)
                    except Exception as e:
                        print(detail_url)
                        print("错误2 %s" % e)
                        self.detail_url_que.put(detail_url)
                        # time.sleep(999)
            else:
                break
    
    def write_2_mdb(self,detailpath,detail_url):
        url = detail_url
        with open(detailpath,encoding='utf-8')as f:
            text = f.read()
            html = Selector(text,'html')
            # 标题
            title = html.xpath("//div[@class='jjianjietitle']/h1/text()").extract_first('')
            title = title.replace('（停刊）','').replace('（Email投稿）','').replace('（Email附打印稿）','').replace('（官网投稿）','').replace('（纸质投稿）','').strip().replace("'","^")
             # 简介
            abstract = ""
            abstracts = html.xpath("//div[@class='jjianjiecon']//text()").extract()
            for i in abstracts:
                abstract += i
            abstract = abstract.replace("...[显示全部]","").replace('\xa0','').replace('\n','').strip().replace("'","^")
            # 投稿方式
            ways = ''
            way = html.xpath("//div[@class='toTouGao']/b/text()").extract_first()
            if way:
                ways = way + ';'
            if not way:
                way = html.xpath("//div[@class='toTouGao']/a/text()").extract()
                if way == []:
                    ways = ''
                else:
                    for w in way:
                        ways += w + ";"
            ways = ways.replace(" ",'')
            table = html.xpath("//div[@class='sjcon']/p")
            for p in table.xpath('string(.)').extract():
                p = p.strip()
            sql = "insert into data (url,期刊名称,简介,投稿方式) values ('%s','%s','%s','%s')" % (url,title,abstract,ways)
            curser = self.db.cursor()
            curser.execute(sql)
            curser.commit()
            print('%s插入成功' % title)

if __name__ == "__main__":
    add_ = eshukan()
    add_.down_list()


