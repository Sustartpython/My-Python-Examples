from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import os, sys, time
import pika
import json
import threading
from random import choice
import redis
import traceback
import queue

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append("./src")
from Const import MQQueueWorker, MQQueueFinish
from mapprovider import MapProvider, Update
from Provider import Provider
import utils

ErrorCheckQueue = queue.Queue()


def ConsumerWorkCallback(channel, method, properties, body):
    task = body.decode('utf-8')
    dic = json.loads(task)
    Provider.workqueue.put(dic)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def ConsumerFinCallback(channel, method, properties, body):
    task = body.decode('utf-8')
    dic = json.loads(task)
    if dic:
        provider = MapProvider.get(dic['provider'])
        if provider:
            func = provider.mapfunc.get(dic['funcname'])
            message = dic['message']
            try:
                func(message)
            except:
                exMsg = '*ConsumerFinCallback: ' + traceback.format_exc()
                utils.logerror(exMsg)
        else:
            exMsg = 'MapProvider has a errorkey:{}'.format(dic['provider'])
            utils.logerror(exMsg)
            print(exMsg)
    else:
        exMsg = ('MQQueueFinish get a errordic')
        utils.logerror(exMsg)
        print(exMsg)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def Work():
    ConnRabbitMQ = Provider.OpenConnRabbitMQ()
    channel = ConnRabbitMQ.channel()  # 创建频道
    channel.basic_qos(prefetch_size=0, prefetch_count=1, global_qos=True)

    # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
    # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
    channel.basic_consume(on_message_callback=ConsumerWorkCallback, queue=MQQueueWorker, auto_ack=False)
    print('Wait Message Work ...')

    channel.start_consuming()


def Finish():
    ConnRabbitMQ = Provider.OpenConnRabbitMQ()
    channel = ConnRabbitMQ.channel()  # 创建频道
    channel.basic_qos(prefetch_size=0, prefetch_count=1, global_qos=True)

    # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
    # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
    channel.basic_consume(on_message_callback=ConsumerFinCallback, queue=MQQueueFinish, auto_ack=False)
    print('Wait Message Finish...')

    channel.start_consuming()


def sender():
    ConnRabbitMQ = Provider.OpenConnRabbitMQ()
    channel = ConnRabbitMQ.channel()  # 创建频道
    while True:
        dic = Provider.messagequeue.get()
        queuetype = dic['queuetype']
        message = dic['message']
        task = json.dumps(message, ensure_ascii=False).encode('utf-8')
        if not ConnRabbitMQ.is_open:
            ConnRabbitMQ = Provider.OpenConnRabbitMQ()
            channel = ConnRabbitMQ.channel()
        if queuetype == 'work':
            try:
                channel.basic_publish(exchange='', routing_key=MQQueueWorker, body=task)
            except pika.exceptions.ConnectionClosed:
                ConnRabbitMQ = Provider.OpenConnRabbitMQ()
                channel = ConnRabbitMQ.channel()
                channel.basic_publish(exchange='', routing_key=MQQueueWorker, body=task)
        elif queuetype == 'finish':
            try:
                channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)
            except pika.exceptions.ConnectionClosed:
                ConnRabbitMQ = Provider.OpenConnRabbitMQ()
                channel = ConnRabbitMQ.channel()
                channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


def exceptionget():
    while True:
        dic, future = ErrorCheckQueue.get()
        try:
            res = future.result()
        except Exception as e:
            # 处理可能出现的异常
            error_msg = e
        else:
            error_msg = ''
        if error_msg:
            print('*** Error for: {}'.format(error_msg))
            utils.logerror('*** Error for: {}'.format(error_msg))
            utils.logerror('{}'.format(dic))


def Main():
    Executor = ThreadPoolExecutor(34)
    Executor.submit(Work)
    Executor.submit(Finish)
    Executor.submit(sender)
    Executor.submit(exceptionget)

    Executor.submit(Update)
    while True:
        dic = Provider.workqueue.get()
        provider = MapProvider.get(dic['provider'])
        if provider:
            func = provider.mapfunc.get(dic['funcname'])
            message = dic['message']
            future = Executor.submit(func, message)
            ErrorCheckQueue.put((dic, future))
        else:
            exMsg = 'MapProvider has a errorkey:{}'.format(dic['provider'])
            utils.logerror(exMsg)
            print(exMsg)


if __name__ == '__main__':
    Main()