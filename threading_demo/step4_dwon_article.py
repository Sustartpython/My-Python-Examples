# -*- coding: utf-8 -*-
# @Time    : 2019/8/19 14:58
# @Author  : qianjun
# @FileName: step4_dwon_article.py
# @Software: PyCharm

# -*- coding: utf-8 -*-
# @Time    : 2019/7/1 11:01
# @Author  : qianjun
# @FileName: down_cnki_bs_article.py
# @Software: PyCharm
"""多线程下载cnki博硕详情页"""
import os, sys, time
import json
import redis
import pymysql
import traceback
import threading
from queue import Queue
import redis
import requests
from bs4 import BeautifulSoup
import os
import pymysql
import traceback
import socket
from random import choice
import configparser

Headers= {

    "Referer":"http://epub.cnki.net/",

    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36"

}
HOST = '192.168.30.199'
PORT = 3306
USER = 'root'
PASSWD = 'vipdatacenter'
DB = 'cnki_bs'
cur_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(cur_path, 'config.ini')

conf = configparser.ConfigParser()
conf.read(config_path, encoding='utf-8')

HtmlRoot = conf.get('FILE', 'htmlListPath')
StartTime = time.time()

if not os.path.exists(HtmlRoot):
    os.makedirs(HtmlRoot)
IPADDR = socket.gethostbyname(socket.gethostname())
PID = os.getpid()
sqlList = []
RedisHost='192.168.30.36'
RedisPort='6379'
RedisDB=0


WorkerThreadNumber =5
GlobalQueueDistribute = Queue(WorkerThreadNumber * 2)  # 分发
GlobalQueueCollect = Queue(WorkerThreadNumber * 2)  # 汇总



# 从数据库任务，分发到本地 worker 线程（本地队列）
class Distributer(threading.Thread):


    def __init__(self):

        print('Distributer __init__ ...')
        super(Distributer, self).__init__()

        self.connDB = pymysql.connect(host=HOST,
                                      user=USER,
                                      passwd=PASSWD,
                                      db=DB,
                                      charset='utf8mb4')
        self.connDB.ping()

        self.connRedis = redis.StrictRedis(host=RedisHost,
                                           port=RedisPort,
                                           db=RedisDB,
                                           decode_responses=True)
        self.taskList = list()

    def __del__(self):
        self.connDB.close()

    # 刷新任务池（从数据库获取任务）
    def updateTaskList(self):
        print('updateTaskList ...')

        self.connDB.ping()

        cur = self.connDB.cursor()


        sql = "select filename,dbcode from latest where stat = 0 and failcount < 10 order by rand() limit 10"

        print('input sql:' + sql)

        cur.execute(sql)
        while True:
            row = cur.fetchone()
            if not row:
                break
            # article_id,title,author,page,beginpage,endpage,date_created,issue_id,jname,jid,eissn,year,mouth,volume,issue
            dic = {
                'filename': row[0].strip(),
                'dbcode': row[1].strip(),


            }

            self.taskList.append(dic)

        self.connDB.commit()
        cur.close()

    def run(self):
        while True:
            if not self.taskList:
                self.updateTaskList()

            print('taskList size: %d' % len(self.taskList))
            if not self.taskList:
                print('taskList is empty!')
                # break
                time.sleep(1)
                continue

            while True:
                print('proxy_iop size: %d' % self.connRedis.scard('proxy_cnki'))

                if self.connRedis.scard('proxy_baidu') < 10:
                    print('Proxy pool too small, reject task! %s' % time.strftime('%H:%M:%S', time.localtime()))
                    time.sleep(1)
                else:
                    proxy = self.connRedis.srandmember('proxy_baidu')
                    break

            task = self.taskList.pop()
            task['proxy'] = proxy  # 给任务添加代理
            print('will put, GlobalQueueDistribute.qsize: %d' % GlobalQueueDistribute.qsize())
            GlobalQueueDistribute.put(task)



# 汇总任务处理状态，回写状态到数据库
class Collecter(threading.Thread):

    def __init__(self):
        print('Collecter __init__ ...')
        super(Collecter, self).__init__()


        self.connDB = pymysql.connect(host=HOST,
                                      user=USER,
                                      passwd=PASSWD,
                                      db=DB,
                                      charset='utf8mb4')

        self.connDB.ping()

    def __del__(self):
        self.connDB.close()

    def run(self):
        while True:

            print("执行任务成功")
            print('will get, GlobalQueueCollect.qsize: %d' % GlobalQueueCollect.qsize())
            sql = GlobalQueueCollect.get()

            print("修改到数据库")
            print('sql: %s' % sql)

            cur = self.connDB.cursor()
            cur.execute(sql)
            self.connDB.commit()
            cur.close()


# 处理具体任务的工人
class Worker(threading.Thread):

    def __init__(self, tid):
        print('Worker __init__ %03d ...' % tid)
        super(Worker, self).__init__()

        self.tid = tid
        self.headers = Headers
        self.proxies = {}
        self.hostname = socket.gethostname()

        self.ip = socket.gethostbyname(self.hostname)
        self.nowDate = time.strftime('%Y%m%d', time.localtime())


    # 目录页是否正确
    def isRightHtml(self, htmlText, doi):

        feature = 'KnowledgeNetLink'
        if htmlText.find(feature) ==-1:
            print("*****特征值*****错误")
            return False
        if htmlText.find(doi) < 0:
            print("*****doi不一致*****错误")
            return False

        if not htmlText.strip().endswith('</html>'):
            print("****not-/html*****")
            return False

        return True

    # 下载一篇文章的详情页和因为数量
    # 成功返回 True；失败返回 False
    def downOnePage(self, doi,dbcode):
        print('thread-%03d-%d, downOnePage %s ...' %
              (self.tid, threading.get_ident(), repr(self.proxies)))
        print(doi,dbcode)
        #url ="http://epub.cnki.net/kns/detail/detail.aspx?dbname=" + "CMFD"+ "&filename=" + doi
        url = "http://epub.cnki.net/kns/detail/detail.aspx?dbname=" +dbcode+ "&filename=" + doi
        print('url: %s' % url)

        r = requests.get(url=url, timeout=(20, 30),proxies=self.proxies,headers=Headers,verify=False)

        print(r)

        # 403 也可能是正常内容
        if r.status_code not in (200,):
            return False


        content = r.content
        if content.startswith(b'\xef\xbb\xbf'):  # 去掉 utf8 bom 头
            content = content[3:]

            #print(content)

        htmlText = content.decode('utf8').strip()
#         with open("./index.html","w",encoding="utf-8") as f:
# #
#             f.write(htmlText)
        # sys.exit(-1)
        if not self.isRightHtml(htmlText, doi):

            return False

        # 每个线程单独写入一个文件，无需加锁
        outPathFile = os.path.join(HtmlRoot,
                    '%s_%s_%d_%03d.big_json' % (self.nowDate, self.ip, os.getpid(), self.tid)
                                       )

        print('Write to %s ...' % outPathFile)
        # doi,title,author,page,beginpage,endpage,date_created,issue_id,jname,jid,eissn,year,mouth,volume,issue

        htmlText = str(doi) + "★" + htmlText.replace("\r", " ").replace("\n", " ").replace("\0", " ") + "\n"

        with open(outPathFile, "a", encoding="utf-8") as f:
            f.write(htmlText)
            print(outPathFile + " Save Success!")

        return True

    def run(self):
        while True:
            print('will get, GlobalQueueDistribute.qsize: %d' % GlobalQueueDistribute.qsize())
            task = GlobalQueueDistribute.get()
            #  article_id,title,author,page,beginpage,endpage,date_created,issue_id,jname,jid,eissn,year,mouth,volume,issue
            doi = task['filename']
            dbcode=task['dbcode']
            if not dbcode:
                decode="CMFD"
            proxy = task['proxy']

            print("代理是：%s"%proxy)

            self.proxies = {
                'http': proxy,
                'https': proxy  # key是指目标网站的协议
            }

            exMsg = None
            rtn = False
            try:
                rtn = self.downOnePage(doi,dbcode)
                #print(rtn)

            except:
                exMsg = '* exMsg:' + traceback.format_exc()

                print(exMsg)

            # if exMsg or (not rtn):  # 处理抛出异常或返回False
            #     sql =  "update latest set failcount = failcount + 1 where filename= '%s' "
            # else:
            #     sql = "update latest set stat = 1 where filename = '%s' "
            #
            # sql = sql % pymysql.escape_string(doi)
            print('will put GlobalQueueCollect.qsize: %d' % GlobalQueueCollect.qsize())
            # GlobalQueueCollect.put(sql)


def Main():
    if not os.path.exists(HtmlRoot):
        os.makedirs(HtmlRoot)


    for tid in range(1, WorkerThreadNumber + 1):
        woker = Worker(tid)
        woker.start()

    Collecter().start()
    Distributer().start()


if __name__ == '__main__':
    Main()

    print('Time total:' + str(int(time.time() - StartTime)) + 's')
    print('Current time:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))