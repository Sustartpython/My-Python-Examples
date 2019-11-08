#encoding: utf-8
#author: walker
#date: 2018-03-05
#summary: 发送/分配任务到任务结点

import os, sys, time
import pika
import pymysql
import json
import threading
from random import choice
from apscheduler.schedulers.background import BackgroundScheduler

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))
'''
插入和删除路径，以导入配置
'''
sys.path.insert(0, os.path.abspath(os.path.join(cur_dir_fullpath, '../../common')))  # 插入
print('*** sys.path: %s' % sys.path)
from common import GolobalConfig
sys.path.pop(0)  # 删除

DBHost = GolobalConfig['ProxyGather']['DBHost']
DBUser = GolobalConfig['ProxyGather']['DBUser']
DBPwd = GolobalConfig['ProxyGather']['DBPwd']
DBName = GolobalConfig['ProxyGather']['DBName']
DBPort = int(GolobalConfig['ProxyGather']['DBPort'])

MQHost = GolobalConfig['mq']['Host']
MQVirtualHost = GolobalConfig['mq']['VirtualHost']
MQUser = GolobalConfig['mq']['User']
MQPwd = GolobalConfig['mq']['Pwd']

MQQueueCenter2Node = GolobalConfig['down_article']['MQQueueCenter2Node']

ConnRabbitMQ = None  # 消息队列连接
ConnDB = None  # 数据库连接
ConnDBLock = threading.Lock()  # 数据库连接锁
TaskPool = list()  # 任务池
ProxyPool = list()  # 代理池

Scheduler = None  #调度器


# 添加并启动定时任务
def InitTimer():
    global Scheduler

    Scheduler = BackgroundScheduler()
    Scheduler.add_job(RefreshProxyPool, 'interval', minutes=1)  # 刷新代理池
    Scheduler.start()


# 打开消息队列连接
def OpenConnRabbitMQ():
    global ConnRabbitMQ
    print('OpenConnRabbitMQ ...')

    credentials = pika.PlainCredentials(MQUser, MQPwd)
    parameters = pika.ConnectionParameters(
        host=MQHost, virtual_host=MQVirtualHost, credentials=credentials, heartbeat_interval=0
    )
    ConnRabbitMQ = pika.BlockingConnection(parameters)  # 连接 RabbitMQ


# 关闭消息队列
def CloseConnRabbitMQ():
    global ConnRabbitMQ
    print('CloseConnRabbitMQ ...')

    ConnRabbitMQ.close()


# 打开数据库连接
def OpenConnDB():
    global ConnDB
    print('OpenConnDB ...')

    ConnDB = pymysql.connect(host=DBHost, user=DBUser, passwd=DBPwd, db=DBName, charset='utf8mb4')
    ConnDB.ping()


# 关闭数据库连接
def CloseConnDB():
    global ConnDB
    print('CloseConnDB ...')

    ConnDB.close()


# 刷新任务池（从数据库获取任务）
def RefreshTaskPool():
    global TaskPool, ConnDBLock
    print('RefreshTaskPool ...')

    with ConnDBLock:
        ConnDB.ping()
        cur = ConnDB.cursor()

        sql = "SELECT url, vol, issue, catalog FROM article WHERE stat=0 AND failcount<9 limit 10000;"
        print('input sql:' + sql)
        cur.execute(sql)
        while True:
            row = cur.fetchone()
            if not row:
                break

            dic = {'url': row[0].strip(), 'vol': row[1].strip(), 'issue': row[2].strip(), 'catalog': row[3].strip()}

            TaskPool.append(dic)
        cur.close()
        ConnDB.commit()


# 刷新代理池（从数据库获取代理）
def RefreshProxyPool():
    global ProxyPool, ConnDBLock
    print('RefreshProxyPool ...')

    with ConnDBLock:
        ConnDB.ping()
        cur = ConnDB.cursor()

        sql = "SELECT proxy FROM proxy_pool;"
        print('input sql:' + sql)
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        ConnDB.commit()

    ProxyPool.clear()
    for item in results:
        ProxyPool.append(item[0])

    print('****** ProxyPool: %d' % len(ProxyPool))
    print('current time:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


# 任务分发
def TaskDistribute():
    global TaskPool

    channel = ConnRabbitMQ.channel()
    while True:  # 循环向队列中发送信息
        while True:
            print('ProxyPool size: %d' % len(ProxyPool))

            if len(ProxyPool) < 10:
                print('Proxy pool too small, reject task! %s' % time.strftime('%H:%M:%S', time.localtime()))
                #time.sleep(1)
                ConnRabbitMQ.sleep(1)
            else:
                proxy = choice(ProxyPool)
                break

        while True:
            # 检查队列，以重新得到消息计数
            queue = channel.queue_declare(queue=MQQueueCenter2Node, passive=True)
            ready = queue.method.message_count

            threshold = len(TaskPool) // 10
            if threshold < 10:
                threshold += 10
            elif threshold > 100:
                threshold = 100
            print(
                'Message ready: %d, TaskPool size: %d, threshold: %d, %s' %
                (ready, len(TaskPool), threshold, time.strftime('%H:%M:%S', time.localtime()))
            )
            if ready < threshold:
                break
            #time.sleep(1)
            ConnRabbitMQ.sleep(1)

        if not TaskPool:  #如果任务池为空，尝试刷新
            RefreshTaskPool()

        if not TaskPool:  #如果刷新后任务池还为空
            print('TaskQueue empty!')
            #time.sleep(1)
            ConnRabbitMQ.sleep(1)
            continue

        dic = TaskPool.pop()
        dic['proxy'] = proxy  # 给任务添加代理
        task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
        channel.basic_publish(exchange='', routing_key=MQQueueCenter2Node, body=task)

        print('send message: %s' % task)


if __name__ == '__main__':
    OpenConnRabbitMQ()  # 打开消息队列
    OpenConnDB()  # 打开数据库

    RefreshProxyPool()  # 从数据库更新一次代理池
    InitTimer()  # 启动定时器，自动更新代理池

    TaskDistribute()  # 分发任务

    CloseConnDB()  # 关闭数据库
    CloseConnRabbitMQ()  # 关闭消息队列
