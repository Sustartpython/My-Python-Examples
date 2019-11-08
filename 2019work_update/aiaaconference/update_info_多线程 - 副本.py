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
import warnings
import datetime
from queue import Queue
from parsel import Selector
from threading import Thread,get_ident

class update_info(object):
    def __init__(self):
        r"""
        定义常量
        """
        self.count = 0
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
        self.uesr = toml.load('config.toml')['USER']
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb, decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_aps'))
        # 读取配置文件获取存储路径
        self.indexPath = toml.load('config.toml')['indexPath']
        self.listPath = toml.load('config.toml')['listPath']
        self.big_jsonPath = toml.load('config.toml')['big_jsonPath']
        self.merge_big_jsonPath = toml.load('config.toml')['mergePath']
        if not os.path.exists(self.indexPath):
            os.makedirs(self.indexPath)
        if not os.path.exists(self.listPath):
            os.makedirs(self.listPath)
        if not os.path.exists(self.big_jsonPath):
            os.makedirs(self.big_jsonPath)
        if not os.path.exists(self.merge_big_jsonPath):
            os.makedirs(self.merge_big_jsonPath)
        #创建抓取多线程队列
        self.que = Queue()
        self.write_que = Queue()
        # 创建队列(将执行详情页抓取成功的url放在该队列中，启用一个单线程对数据库更新)
        self.sql_que = Queue()
        #链接数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        #创建big_json文件夹
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        os.chdir(self.big_jsonPath)
        self.pwd = os.getcwd()+"\\"+self.now_time
        self.big_json_path = os.path.exists(self.pwd)
        # 判断文件是否存在：不存在创建
        if not self.big_json_path:
            os.makedirs(self.pwd)

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
        count_1st = 0
        while count_1st < 20:
            proxy_one = {
                # 'https':random.choice(self.proxieslist)
            }
            try:
                res_one = requests.get(url=self.url, headers=self.headers, proxies=proxy_one, timeout=60)
                if res_one.status_code == 200:
                    with open(path, mode='w', encoding='utf-8') as f:
                        f.write(res_one.content.decode('utf8'))
                        print("下载首页成功")
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
        print("总页数为%s"%totalpage)
        for page in range(0,totalpage):
            fname = self.indexPath + '/%s.html' % page
            url = "https://arc.aiaa.org/action/showPublications?pubType=meetingProc&startPage=%s&pageSize=100" % page
            while True:
                proxy_two = {
                    # 'https': random.choice(self.proxieslist)
                }
                try:
                    res_two = requests.get(url, headers=self.headers, proxies=proxy_two, timeout=20)
                    if res_two.status_code == 200:
                        with open(fname, mode='w', encoding='utf-8') as fw:
                            fw.write(res_two.content.decode('utf8'))
                        print("%s down right!" % url)
                        self.down_list(fname)
                        break
                    else:
                        print("%s------ip error and retry!" % url)
                        continue
                except Exception as e:
                    print(e)
                    print("%s------timeout and retry!" % url)
                    continue

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
            sql = "insert ignore into lists(url,id,meeting_name) VALUES (%s,%s,%s)"
            message = (list_url,name,title)
            cur = self.conn.cursor()
            cur.execute(sql,message)
            self.conn.commit()
            continue
        while True:
            proxy = {
            # 'https': random.choice(self.proxieslist)
        }
            sql = "select url,id from lists where stat = 0"
            urls = self.read_sql(sql)
            if urls == ():
                return
            else:
                for urlxx in urls:
                    urlx = urlxx[0]
                    name = urlxx[1]
                    list_fname = self.listPath + '/%s.html' % name
                    print(urlx)
                    try:
                        res = requests.get(urlx, headers=self.headers, proxies=proxy, timeout=120)
                        if res.status_code == 200:
                            with open(list_fname, mode='w', encoding='utf-8') as f:
                                f.write(res.content.decode('utf8'))
                            print("%s下载成功!" % urlx)
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
               
    def get_sqlurl(self):
        # 获取URL
        sql = "select url,session,eisbn from detail where stat = 0"
        urls = self.read_sql(sql)
        print("此次更新的数据为%s条"%len(urls))
        if urls == ():
            print("暂时无需更新！！！")
            msg = "无更新，不上传"
            self.msg2weixin(msg)
        else:
            self.count = len(urls)
            for urlxx in urls:
                self.que.put(urlxx)          
        
    def get_data(self):
        r"""
        获取详情页信息
        """
        while True:
            proxy = {
                    'https': random.choice(self.proxieslist)
                }
            if not self.que.empty():
                lists = self.que.get()
                url = lists[0]
                session = lists[1]
                eisbn = lists[2]
                try:
                    res = requests.get(url=url, headers=self.headers,proxies=proxy,timeout=60)
                    if res.status_code == 200:
                        text_one = res.content.decode('utf8').strip()
                        sumDict = dict()
                        sumDict['url'] = url
                        sumDict['session'] = session
                        sumDict['eisbn'] = eisbn
                        sumDict['down_date'] = self.now_time
                        sumDict['htmlText'] = text_one
                        big_json_name =self.pwd + '/%s_%s_%s.big_json' % (self.now_time,os.getpid(),get_ident())
                        with open(big_json_name, mode='a', encoding='utf-8') as f:
                            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                            f.write(line)
                            self.sql_que.put(url)
                        self.sql_que.put(url)
                        print('%s写入成功'%url)
                    else:
                        print("%s------ip error and retry!" % url)
                        self.que.put(lists)
                except Exception as e:
                    print(e)
                    print("%s------timeout and retry!" % url)
                    self.que.put(lists)
            else:
                break

    def update_sqlque(self):
        while True:
            if not self.sql_que.empty():
                url = self.sql_que.get()
                sql = "update detail set stat = 1 where url = '%s'" % (url)
                self.update_sql(sql)
            else:
                break

    def all_2_one(self):
        r"""
        合并单个文件到一个大文件中
        """
        new_file = self.merge_big_jsonPath + '/' + self.now_time + '_' + repr(random.randrange(111, 999)) + ".big_json"
        for root, dirs, files in os.walk(self.pwd):
            for file in files:
                filename = os.path.join(root, file)
                print(filename)
                with open(filename, mode='r', encoding="utf-8") as fp:
                    text = fp.readline()
                    while text:
                        with open(new_file, mode='a', encoding="utf-8") as f:
                            f.write(text)
                        text = fp.readline()
        self.up_hdfs(new_file)
    

    def up_hdfs(self,new_file):
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
        local_path = new_file
        up_path = HdfsDir + '/%s.big_json' % (now_time)
        big_json_size = os.path.getsize(local_path)
        if big_json_size != 0 :
            if client.exists(up_path):
                client.delete(up_path)
            client.copy_from_local(local_path,up_path)
            print('After !%s' % client.listdir(HdfsDir))
            msg = '成功上传到%s' % HdfsDir
            self.msg2weixin(msg)
        else:
            print("无更新，不上传")
            
    def msg2weixin(self,msg):
        Headers = {
        'Accept':
            '*/*',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        }

        corpid = r'wwa7df1454d730c823'
        corpsecret = r'dDAusBg3gK7hKhLfqIRlyp84UDtII6NkMW7s8Wn2wgs'
        url = r'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s' % (corpid, corpsecret)
        count = 0
        while count < 3:
            try:
                r = requests.get(url)
                content = r.content.decode('utf8')
                dic = json.loads(content)
                accessToken = dic['access_token']
                url = r'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s' % accessToken
                form = {"touser": self.user, "msgtype": "text", "agentid": 1000015, "text": {"content": msg}, "safe": 0}

                r = requests.post(url=url, data=json.dumps(form), headers=Headers, timeout=30)
                break
            except:
                count += 1
                print('发送消息到企业微信失败')

    def main(self):
        self.down_index()
        self.get_sqlurl()
        # 空列表
        t_list = []
        # 创建多个线程并启动线程
        for i in range(50):
            t = Thread(target=self.get_data)
            t_list.append(t)
            t.start()
        # 回收线程
        for i in t_list:
            i.join()
        self.update_sqlque()
        if self.count:
            self.all_2_one()



if __name__ == "__main__":
    news = update_info()
    news.main()
                





        
        
