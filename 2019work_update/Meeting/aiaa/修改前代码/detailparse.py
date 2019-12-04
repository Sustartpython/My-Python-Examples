import os
import re
import time
import json
import toml
import redis
import pyhdfs
import random
import requests
from queue import Queue
from parsel import Selector
from threading import Thread

class detailparse(object):
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1'
        }
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb, decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_jama'))
        # 读取配置文件获取存储路径
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']
        # 创建队列
        self.q = Queue()
        self.urlnumber = 0
        self.i = 0

    def get_filepath(self):
        for parent, dirnames, filenames in os.walk(self.listPath, followlinks=True):
            for filename in filenames:
                file_path = os.path.join(parent, filename)
                self.get_url(file_path)

    def get_url(self,file_path):
        with open(file_path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        result = html.xpath("//h5[@class='issue-item__title']/a/@href").getall()
        if result is None:
            return
        else:
            for i, item in enumerate(result):
                try:
                    lists = []
                    self.i +=1
                    url = 'https://arc.aiaa.org' + item
                    name = re.findall('10.2514/(.*)', item)[0]
                    session = html.xpath("//div[@class='issue-item__session_details']/text()").getall()[i]
                    lists = [url,name,session]
                except:
                    lists = [url,name,""]
                self.q.put(lists)
            print(self.i)

    def get_data(self):
        while True:
            proxy = {
                'https': random.choice(self.proxieslist)
            }

            # 获取URL
            if not self.q.empty():
                lists = self.q.get()
                url = lists[0]
                name = lists[1]
                session = lists[2]
                detail_name = self.detailPath + '/%s.html' % name
                if os.path.exists(detail_name):
                    sumDict = dict()
                    with open(detail_name,encoding='utf-8') as f:
                        text_one = f.read()
                    sumDict['url'] = url
                    sumDict['session'] = session
                    sumDict['htmlText'] = text_one
                    with open(r'E:\work\Meeting\aiaa\big_json\20190803.big_json', 'a', encoding='utf-8') as fw:
                        line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                        fw.write(line)
                    time.sleep(1)
                    continue
                print('offline:%s写入成功' % detail_name)
                try:
                    res = requests.get(url=url, headers=self.headers,proxies=proxy,timeout=60)
                    print(detail_name)
                    print(res.status_code)
                    print('--------')
                    if res.status_code == 200:
                        with open(detail_name, mode='w', encoding='utf-8') as f:
                            f.write(res.content.decode('utf8'))
                        sumDict = dict()
                        with open(detail_name, encoding='utf-8') as f:
                            text_one = f.read()
                        sumDict['url'] = url
                        sumDict['htmlText'] = text_one
                        with open(r'E:\work\Meeting\aiaa\big_json\20190801.big_json', 'a', encoding='utf-8') as fw:
                            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                            fw.write(line)
                            print('online:%s写入成功' % detail_name)
                    else:
                        print('ip!!!')
                        self.q.put(lists)
                except:
                    print("time out ")
                    print(detail_name)
                    print('--------')
                    self.q.put(lists)
            else:
                break

    def up_hdfs(self):
        #链接hdfs
        client = pyhdfs.HdfsClient(hosts='hadoop2x-01:50070,hadoop2x-02:50070',user_name='suh')
        # 从本地上传文件至集群之前，集群的目录
        print('Before !%s' % client.listdir('/RawData/aiaa_meeting/big_json/2019/20190802'))
        #从本地上传文件至集群
        client.copy_from_local(r'E:\work\Meeting\aiaa\big_json\20190801.big_json','/RawData/aiaa_meeting/big_json/2019/20190802/20190802.big_json')
        # 从本地上传文件至集群之后，集群的目录
        print('After !%s' % client.listdir('/RawData/aiaa_meeting/big_json/2019/20190802'))

    def main(self):
        self.get_filepath()
        # 空列表
        t_list = []
        # 创建多个线程并启动线程
        for i in range(100):
            t = Thread(target=self.get_data)
            t_list.append(t)
            t.start()
        # 回收线程
        for i in t_list:
            i.join()
        print(self.i)
        self.up_hdfs()


if __name__ == '__main__':
    detailp = detailparse()
    detailp.main()


