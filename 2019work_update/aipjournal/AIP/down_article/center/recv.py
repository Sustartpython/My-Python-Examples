#encoding: utf-8
#author: walker
#date: 2018-03-05
#summary: 接收任务结点返回的sql并更新到数据库

import os, sys
import pika
import pymysql

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

MQQueueNode2Center = GolobalConfig['down_article']['MQQueueNode2Center']

ConnRabbitMQ = None  # 消息队列连接
ConnDB = None


# 打开消息队列连接
def OpenConnRabbitMQ():
    global ConnRabbitMQ
    print('OpenConnRabbitMQ ...')

    credentials = pika.PlainCredentials(MQUser, MQPwd)
    parameters = pika.ConnectionParameters(
        host=MQHost, virtual_host=MQVirtualHost, credentials=credentials, heartbeat_interval=0
    )
    ConnRabbitMQ = pika.BlockingConnection(parameters)  # 连接 RabbitMQ


# 关闭消息队列连接
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


# 接收处理消息的回调函数
def ConsumerCallback(channel, method, properties, body):
    global ConnDB

    sql = body.decode('utf-8')
    print('recv body: %s' % sql)

    ConnDB.ping()
    cur = ConnDB.cursor()
    cur.execute(sql)
    ConnDB.commit()
    cur.close()

    # 向 Center2Node 发送 message 消费信息
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print()


def Main():
    channel = ConnRabbitMQ.channel()  # 创建频道
    channel.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=True)

    # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
    # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
    channel.basic_consume(ConsumerCallback, queue=MQQueueNode2Center, no_ack=False)
    print('Wait Message ...')

    channel.start_consuming()


if __name__ == '__main__':
    OpenConnRabbitMQ()
    OpenConnDB()
    Main()
    CloseConnDB()
    CloseConnRabbitMQ()