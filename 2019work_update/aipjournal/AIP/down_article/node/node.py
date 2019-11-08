#encoding: utf-8
#author: walker
#date: 2018-03-05
#summary: 发送/分配任务到任务结点

import os, sys, time
import pika
import pymysql
import json
import requests
import traceback
import threading
from queue import Queue
import socket

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))
'''
插入和删除路径，以导入配置
'''
sys.path.insert(0, os.path.abspath(os.path.join(cur_dir_fullpath, '../../common')))  # 插入
print('*** sys.path: %s' % sys.path)
from common import GolobalConfig
sys.path.pop(0)  # 删除

MQHost = GolobalConfig['mq']['Host']
MQVirtualHost = GolobalConfig['mq']['VirtualHost']
MQUser = GolobalConfig['mq']['User']
MQPwd = GolobalConfig['mq']['Pwd']

MQQueueCenter2Node = GolobalConfig['down_article']['MQQueueCenter2Node']
MQQueueNode2Center = GolobalConfig['down_article']['MQQueueNode2Center']

ArticleRoot = GolobalConfig['down_article']['ArticleRoot']
WorkerThreadNumber = int(GolobalConfig['down_article']['WorkerThreadNumber'])

GlobalQueueDistribute = Queue(WorkerThreadNumber * 10)  # 分发
GlobalQueueCollect = Queue(WorkerThreadNumber * 10)  # 汇总

Headers = {
    'Accept': '*/*',
    'Referer': 'http://www.cnki.net/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'
}


def GetLocalIPByPrefix(prefix):
    localIP = ''
    for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
        if ip.startswith(prefix):
            localIP = ip

    return localIP


# 汇总任务处理状态，发送 sql 语句回 center
class Collecter(threading.Thread):

    def __init__(
        self,
    ):
        print('Collecter __init__ ...')
        super(Collecter, self).__init__()
        self._params = pika.ConnectionParameters(
            host=MQHost,
            virtual_host=MQVirtualHost,
            credentials=pika.PlainCredentials(MQUser, MQPwd),
            heartbeat_interval=0
        )

        self._conn = None
        self._channel = None

    def __del__(self):
        self._channel.close()
        self._conn.close()

    # 连接 RabbitMQ
    def _connect(self):
        self._conn = pika.BlockingConnection(self._params)  # 创建连接
        self._channel = self._conn.channel()  # 创建频道

    def run(self):
        while True:
            sql = GlobalQueueCollect.get()

            if (not self._conn) or (not self._conn.is_open):
                self._connect()

            try:
                self._channel.basic_publish(exchange='', routing_key=MQQueueNode2Center, body=sql)
            except pika.exceptions.ConnectionClosed:
                print('reconnect for ConnectionClosed')
                self._connect()
                self._channel.basic_publish(exchange='', routing_key=MQQueueNode2Center, body=sql)

            print('send message: %s' % sql)


# 从 RabbitMQ 接收任务，分发到本地 worker 线程（本地队列）
class Distributer(threading.Thread):

    def __init__(
        self,
    ):
        print('Distributer __init__ ...')
        super(Distributer, self).__init__()

        self._params = pika.ConnectionParameters(
            host=MQHost,
            virtual_host=MQVirtualHost,
            credentials=pika.PlainCredentials(MQUser, MQPwd),
            heartbeat_interval=0
        )

        self._conn = None
        self._channel = None

    def __del__(self):
        self._channel.close()
        self._conn.close()

    # 连接 RabbitMQ
    def _connect(self):
        self._conn = pika.BlockingConnection(self._params)  # 创建连接
        self._channel = self._conn.channel()  # 创建频道

        #self._channel.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=True)
        self._channel.basic_qos(prefetch_size=0, prefetch_count=WorkerThreadNumber, all_channels=True)

    def run(self):
        while True:
            if (not self._conn) or (not self._conn.is_open):
                self._connect()

            method, properties, body = self._channel.basic_get(MQQueueCenter2Node)

            if not body:
                print('GlobalQueueDistribute.qsize: %d' % GlobalQueueDistribute.qsize())
                print('GlobalQueueCollect.qsize: %d' % GlobalQueueCollect.qsize())
                duration = 10
                print('not body, Distributer thread will sleep %ds ...' % duration)
                self._conn.sleep(duration)
                continue

            task = body.decode('utf-8')
            print('recv body: %s' % task)
            print('GlobalQueueDistribute.qsize: %d' % GlobalQueueDistribute.qsize())
            GlobalQueueDistribute.put(task)
            self._channel.basic_ack(delivery_tag=method.delivery_tag)


# 处理具体任务的工人
class Worker(threading.Thread):

    def __init__(self, tid):
        print('Worker __init__ %03d ...' % tid)
        super(Worker, self).__init__()

        self.tid = tid
        self.headers = Headers
        self.proxies = {}
        self.nodeIP = GetLocalIPByPrefix('192.168.')
        self.nowDate = time.strftime('%Y%m%d', time.localtime())

    # 详情页是否正确
    def isRightDetail(self, filename, htmlText):
        if htmlText.find(filename) < 0:
            return False
        feature = 'class="title"'
        if htmlText.find(feature) < 0:
            return False

        lowerText = htmlText.lower()
        if (lowerText.find('layer7') > -1) and (lowerText.find('ddos') > -1):
            return False

        return True

    # refcount 是否正确
    def isRightRefcount(self, dic):
        if 'REFERENCE' not in dic:  # 引文量
            return False
        if 'CITING' not in dic:  # 被引量
            return False
        return True

    # 下载一篇文章的详情页
    # 成功返回 True；失败返回 False
    def downOnePage(self, url, vol, issue, catalog):
        print('thread-%03d-%d, downOnePage %s, %s ...' % (self.tid, threading.get_ident(), url, repr(self.proxies)))
        urlsplist = url.split('/')
        article_dir = ArticleRoot + '/' + catalog + '/' + vol
        article_file = article_dir + '/' + issue + '_' + urlsplist[-2] + '_' + urlsplist[-1] + '.html'
        if os.path.exists(article_file):
            return True

        feature = 'class="publicationContentTitle"'

        # 外层捕获异常
        r = requests.get(url=url, proxies=self.proxies, timeout=(10, 20))

        if r.status_code != 200:
            return False
        htmlText = r.content.decode('utf8').strip()
        if htmlText.find(feature) < 0:
            return False
        if htmlText.find('</html>') < 0:
            return False

        sumDict = dict()
        sumDict['url'] = url
        sumDict['vol'] = vol
        sumDict['issue'] = issue
        sumDict['catalog'] = catalog
        sumDict['doi'] = urlsplist[-2] + '/' + urlsplist[-1]
        sumDict['detail'] = htmlText

        # 每个线程单独写入一个文件，无需加锁
        outPathFile = os.path.join(
            ArticleRoot, '%s_%s_%d_%03d.big_json' % (self.nowDate, self.nodeIP, os.getpid(), self.tid)
        )
        print('Write to %s ...' % outPathFile)
        with open(outPathFile, mode='a', encoding='utf-8') as f:
            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
            f.write(line)
        return True

    def run(self):
        while True:
            task = GlobalQueueDistribute.get()
            #print('%s, task: %s\n' % (self.outFile, task))

            dic = json.loads(task)

            #print("Received %s" % repr(dic))

            url = dic['url']
            vol = dic['vol']
            issue = dic['issue']
            catalog = dic['catalog']
            proxy = dic['proxy']
            self.proxies = {
                'http': proxy,
                'https': proxy  #key是指目标网站的协议
            }

            exMsg = None
            rtn = False
            try:
                rtn = self.downOnePage(url, vol, issue, catalog)
            except:
                exMsg = '* exMsg:' + traceback.format_exc()
                print(exMsg)

            if exMsg or (not rtn):  # 处理抛出异常或返回False
                sql = "UPDATE article SET failcount=failcount+1 WHERE url='%s';"
            else:
                sql = "UPDATE article SET stat=1 WHERE url='%s';"

            sql = sql % pymysql.escape_string(url)
            print('GlobalQueueCollect.qsize: %d' % GlobalQueueCollect.qsize())
            GlobalQueueCollect.put(sql)


def Main():
    if not os.path.exists(ArticleRoot):
        input('%s not found' % ArticleRoot)
        sys.exit(-1)

    for tid in range(1, WorkerThreadNumber + 1):
        woker = Worker(tid)
        woker.start()

    Collecter().start()
    Distributer().start()


if __name__ == '__main__':
    print('main thread id: %d' % threading.get_ident())
    Main()
