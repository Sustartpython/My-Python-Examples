import json
import time
import os
import redis
import queue
from random import choice
from Const import RDHost, RDPort, RDDb, MQUser, MQPwd, MQHost, MQVirtualHost, MQQueueWorker, MQQueueFinish
import pika
import utils
import sys

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))


class Provider():
    workqueue = queue.Queue()
    messagequeue = queue.Queue()

    # 打开消息队列连接
    @staticmethod
    def OpenConnRabbitMQ():
        print('OpenConnRabbitMQ ...')

        credentials = pika.PlainCredentials(MQUser, MQPwd)
        parameters = pika.ConnectionParameters(
            host=MQHost, virtual_host=MQVirtualHost, credentials=credentials, heartbeat=0
        )
        return pika.BlockingConnection(parameters)  # 连接 RabbitMQ

    def __init__(self, provider, proxypoolname=False, hdfsroot=None):
        self.provider = provider
        self.proxypool = list()
        self.proxypoolname = proxypoolname
        self.datepath = None
        self.html_path = None
        self.index_path = None
        self.list_path = None
        self.detail_path = None
        self.cover_path = None
        self.hdfsroot = hdfsroot
        self.hdfs_path = None
        self.merge_path = None
        self.template_file = None
        self.conn = None
        self.db3_path = os.path.abspath(os.path.join(cur_dir_fullpath, r"..\db3"))
        if not os.path.exists(self.db3_path):
            os.makedirs(self.db3_path)

        #函数名与函数地址的dic
        self.mapfunc = {
            'refreshproxypool': self.refreshproxypool,
            'initpath': self.initpath,
            'upload2HDFS': self.upload2HDFS
        }
        if self.proxypoolname:
            self.refreshproxypool()

    def refreshproxypool(self):
        if self.proxypoolname:
            r = redis.StrictRedis(host=RDHost, port=RDPort, db=RDDb, decode_responses=True)
            results = r.smembers(self.proxypoolname)
            self.proxypool.clear()
            for item in results:
                self.proxypool.append(item)
        else:
            utils.printf('{}:Do not exist proxypoolname'.format(self.provider))

    def getproxy(self):
        cnt = 0
        while len(self.proxypool) < 10:
            print('Proxy pool too small, reject task! %s' % time.strftime('%H:%M:%S', time.localtime()))
            time.sleep(1)
            cnt += 1
            if cnt == 100:
                self.senddistributefinish('refreshproxypool')
                cnt = 0
        return choice(self.proxypool)

    def package(self, funcname, message=None):
        #打包消息
        dic = {}
        dic['provider'] = self.provider
        if self.mapfunc.get(funcname):
            dic['funcname'] = funcname
        else:
            print('%s can not find func: %s' % (self.provider, funcname))
            utils.logerror('%s can not find func: %s' % (self.provider, funcname))
            return None
        dic['message'] = message
        return dic

    def sendwork(self, funcname, message=None):
        #向本地工作队列发送消息,funcname:工作线程需要调用的函数名字,message:函数要用到的参数
        dic = self.package(funcname, message)
        if dic:
            self.workqueue.put(dic)

    def senddistributework(self, funcname, message=None):
        #向远程工作队列发送消息
        pack = self.package(funcname, message)
        if pack:
            dic = {}
            dic['queuetype'] = 'work'
            dic['message'] = pack
            self.messagequeue.put(dic)

    def senddistributefinish(self, funcname, message=None):
        #向远程完成队列发送消息
        pack = self.package(funcname, message)
        if pack:
            dic = {}
            dic['queuetype'] = 'finish'
            dic['message'] = pack
            self.messagequeue.put(dic)

    def initpath(self):
        #指定任务时需要初始化工作目录
        rootpath = os.path.abspath(os.path.join(cur_dir_fullpath, r"..\mission", self.provider))
        new_dirname = time.strftime("%Y%m%d")
        self.datepath = os.path.join(rootpath, new_dirname)
        self.html_path = os.path.join(self.datepath, 'html')
        self.index_path = os.path.join(self.datepath, 'index')
        self.list_path = os.path.join(self.datepath, 'list')
        self.detail_path = os.path.join(self.datepath, 'detail')
        self.cover_path = os.path.join(self.datepath, 'cover')
        self.merge_path = os.path.join(self.datepath, 'merge')

        if self.hdfsroot:
            self.hdfs_path = self.hdfsroot + '/' + new_dirname[0:4] + '/' + new_dirname
            if not os.path.exists(self.merge_path):
                os.makedirs(self.merge_path)

        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        if not os.path.exists(self.index_path):
            os.makedirs(self.index_path)
        if not os.path.exists(self.list_path):
            os.makedirs(self.list_path)
        if not os.path.exists(self.detail_path):
            os.makedirs(self.detail_path)
        if not os.path.exists(self.cover_path):
            os.makedirs(self.cover_path)

    def predb3(self, db3name='zt_template.db3', fname=None):
        #将模版文件拷贝到db3文件夹改名,建立连接
        if not self.datepath:
            self.initpath()
        src = os.path.join(self.db3_path, 'template', db3name)
        if not os.path.exists(src):
            utils.printf('can not find %s' % src)
            utils.logerror('can not find %s' % src)
            return
        if fname:
            filename = '%s_%s.db3' % (fname, self.datepath.split('\\')[-1])
        else:
            filename = '%s_%s.db3' % (self.provider, self.datepath.split('\\')[-1])
        self.template_file = os.path.join(self.db3_path, filename)
        import shutil
        shutil.copy(src, self.template_file)
        self.conn = utils.init_db('sqlite3', self.template_file)

    def upload2HDFS(self, message):
        if not self.hdfs_path:
            utils.logerror('%s:hdfs_path未初始化' % self.provider)
            return
        utils.all_2_one(self.detail_path, self.merge_path)
        flag = utils.ProcAll(self.merge_path, self.hdfs_path)
        if flag:
            utils.msg2weixin('%s:bigjson成功上传至%s' % (self.provider, self.hdfs_path))
        else:
            utils.logerror('%s:bigjson上传至%s出现问题' % (self.provider, self.hdfs_path))
            utils.msg2weixin('%s:bigjson上传至%s出现问题' % (self.provider, self.hdfs_path))