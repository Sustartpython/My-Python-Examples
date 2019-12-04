from parsel import Selector
import time
import requests
import utils
import redis
import pypyodbc
import pymysql
import random
import traceback
import threading
from queue import Queue

WorkerThreadNumber =5
GlobalQueueDistribute = Queue(WorkerThreadNumber * 2)  # 分发
GlobalQueueCollect = Queue(WorkerThreadNumber * 2)  # 汇总

HOST = '192.168.30.209'
PORT = 3306
USER = 'root'
PASSWD = 'vipdatacenter'
DB = 'clc_no'

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
        redisHost = r'192.168.30.36'
        redisPort = r'6379'
        redisDb = r'0'
        self.connRedis = redis.StrictRedis(host=redisHost,
                                           port=redisPort,
                                           db=redisDb,
                                           decode_responses=True)
        self.taskList = list()
    
    # 刷新任务池（从数据库获取任务）
    def updateTaskList(self):
        print('updateTaskList ...')

        cur = self.connDB.cursor()
        sql = "select fcode,info from data where stat = 0"
        print('input sql:' + sql)
        cur.execute(sql)
        while True:
            row = cur.fetchone()
            if not row:
                break
            dic = {
                'fcode': row[0].strip(),
                'info': row[1].strip(),
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
        self.proxies = {}
    def parse(self,fcode):
        url = "http://www.clcindex.com/category/%s/" % fcode
        try:
            res = requests.get(url,proxies=self.proxies,timeout=3)
            if res.status_code == 200:
                print(url)
                print(self.proxies)
                html = Selector(res.text,'html')
                clc_nos = html.xpath("//tr[@name='item-row']//td[2]/text()").extract()
                for i,clc_no in enumerate(clc_nos):
                    clc_no = clc_no.replace("\t","").replace("\n","")
                    clc_name = html.xpath("//tr[@name='item-row']//td[3]//text()").extract()[i].replace("\t","").replace("\n","")
                    sql = "insert ignore into clc(fcode,info) values ('%s','%s')" % (clc_no,clc_name)
                    connDB = pymysql.connect(host=HOST,
                                      user=USER,
                                      passwd=PASSWD,
                                      db=DB,
                                      charset='utf8mb4')
                    curser = connDB.cursor()
                    curser.execute(sql)
                    connDB.commit()
                    utils.printf("%s 插入成功" % clc_no)
                return True
            else:
                return False
        except Exception as e:
            return False

    def run(self):
        while True:
            print('will get, GlobalQueueDistribute.qsize: %d' % GlobalQueueDistribute.qsize())
            task = GlobalQueueDistribute.get()
            #  article_id,title,author,page,beginpage,endpage,date_created,issue_id,jname,jid,eissn,year,mouth,volume,issue
            fcode = task['fcode']
            info = task['info']
            proxy = task['proxy']

            print("代理是：%s"%proxy)
            self.proxies = {
                'http': proxy,
            }

            exMsg = None
            rtn = False
            try:
                rtn = self.parse(fcode)
                print(rtn)

            except:
                exMsg = '* exMsg:' + traceback.format_exc()
                print(exMsg)
            if exMsg or (not rtn):  # 处理抛出异常或返回False
                print("try again")
            else:
                sql = "update clc set stat = 1 where fcode = '%s' "
            print('will put GlobalQueueCollect.qsize: %d' % GlobalQueueCollect.qsize())
            # print(sql)
            # GlobalQueueCollect.put(sql)

def Main():

    for tid in range(1, WorkerThreadNumber + 1):
        woker = Worker(tid)
        woker.start()
    
    Distributer().start()
    Collecter().start()
       

if __name__ == "__main__":
    Main()

    