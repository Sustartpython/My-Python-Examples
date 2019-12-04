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

class write_big_json(object):
    def __init__(self):
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
                # print(file_path)
                self.get_url(file_path)

    def get_url(self,file_path):
        with open(file_path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        result = html.xpath("//h5[@class='issue-item__title']/a/@href").getall()
        if result is None:
            return
        else:
            fw = open(r'E:\work\Meeting\aiaa\big_json\20190803.big_json', 'a', encoding='utf-8')
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
                self.write_info(lists,fw)

    def write_info(self,lists,fw):
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
            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
            fw.write(line)
            print('offline:%s写入成功' % detail_name)
        else:
            print(lists)
            time.sleep(120)

    def update_json(self,lists,eISBN,fw):
        print(1)
        for line in fw:
                print(line)



    def up_hdfs(self):
        #链接hdfs
        client = pyhdfs.HdfsClient(hosts='hadoop2x-01:50070,hadoop2x-02:50070',user_name='suh')
        # 从本地上传文件至集群之前，集群的目录
        print('Before !%s' % client.listdir('/RawData/aiaa_meeting/big_json/2019/20190803'))
        #从本地上传文件至集群
        client.copy_from_local(r'E:\work\Meeting\aiaa\big_json\20190803.big_json','/RawData/aiaa_meeting/big_json/2019/20190803/20190803.big_json')
        # 从本地上传文件至集群之后，集群的目录
        print('After !%s' % client.listdir('/RawData/aiaa_meeting/big_json/2019/20190803'))




if __name__ == '__main__':
    detailp = write_big_json()
    begin = time.time()
    detailp.get_filepath()
    print('current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    # detailp.up_hdfs()
    print('current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
