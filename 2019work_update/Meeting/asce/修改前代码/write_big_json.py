import os
import re
import time
import json
import toml
import redis
import pyhdfs
import random
import requests
from parsel import Selector


class write_big_json(object):
    def __init__(self):
        # 读取配置文件获取存储路径
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']
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
        result = html.xpath("//div[@class='tocList__title clearIt']//a[@class='ref nowrap']/@href").getall()
        isbn = html.xpath("//span[@class='bookInfo__isbn__print']/text()").getall()
        eisbn = html.xpath("//span[@class='bookInfo__isbn__pdf']/text()").getall()
        pages = html.xpath("//div[@class='tocList__pages']/text()").getall()
        accept_date = html.xpath("//span[@class='conf-date']/text()").extract_first('')
        meeting_name = html.xpath("//h1[@class='bookInfo__title']/text()").extract_first('')
        meeting_place = html.xpath("//span[@class='conf-loc']//text()").extract_first('')
        if result is None:
            return
        else:
            fw = open(r'D:\code\proceedings\big_json\20190812_1.big_json', 'a', encoding='utf-8')
            for i, item in enumerate(result):
                lists = []
                self.i +=1
                url = 'https://ascelibrary.org' + item
                name = re.findall('10.1061/(.*)', item)[0]
                page = pages[i]
                if eisbn != []:
                    if "ISBN (PDF)" in eisbn[0]:
                        eisbns = eisbn[0].replace("ISBN (PDF):","").replace("-","").strip()
                    else:
                        eisbns = ""
                else:
                    eisbns = ""
                if isbn != []:
                    if "ISBN (print)" in isbn[0]:
                        isbns = isbn[0].replace("ISBN (print):","").replace("-","").strip()
                    else:
                        isbns = ""
                else:
                    isbns = ""
                if meeting_place == "":
                    name_place = html.xpath("//div[@class='conf-date-loc']/text()").getall()[0]
                    temp = name_place.split("|")
                    meeting_place = temp[1]
                    accept_date = temp[0]
                lists = [url,name,isbns,eisbns,page,accept_date,meeting_name,meeting_place]
                self.write_info(lists,fw)

    def write_info(self,lists,fw):
        url = lists[0]
        name = lists[1]
        isbn = lists[2]
        eisbn = lists[3]
        pages = lists[4]
        accept_date = lists[5]
        meeting_name = lists[6]
        meeting_place = lists[7]
        detail_name = self.detailPath + '/%s.html' % name
        if os.path.exists(detail_name):
            sumDict = dict()
            with open(detail_name,encoding='utf-8') as f:
                text_one = f.read()
            html_two = Selector(text_one, type='html')
            keyword = html_two.xpath("//section//a/text()").getall()
            if keyword !=[]:
                keywords = ""
                for i in keyword:
                    keywords += i + ";"
            else:
                keywords = ""
            sumDict['url'] = url
            sumDict['meeting_name'] = meeting_name
            sumDict['meeting_place'] = meeting_place
            sumDict['accept_date'] = accept_date 
            sumDict['isbn'] = isbn
            sumDict['eisbn'] = eisbn
            sumDict['pages'] = pages
            sumDict['keyword'] = keywords
            sumDict['htmlText'] = text_one
            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
            fw.write(line)
            print('offline:%s写入成功' % detail_name)
        else:
            print(lists)
            time.sleep(9999999)


    def up_hdfs(self):
        #链接hdfs
        client = pyhdfs.HdfsClient(hosts='hadoop2x-01:50070,hadoop2x-02:50070',user_name='suh')
        # 从本地上传文件至集群之前，集群的目录
        print('Before !%s' % client.listdir('/RawData/asceproceedings/big_json/2019/20190812'))
        #从本地上传文件至集群
        client.copy_from_local(r'D:\code\proceedings\big_json\20190812_1.big_json','/RawData/asceproceedings/big_json/2019/20190812/20190812_1.big_json')
        # 从本地上传文件至集群之后，集群的目录
        print('After !%s' % client.listdir('/RawData/asceproceedings/big_json/2019/20190812'))




if __name__ == '__main__':
    detailp = write_big_json()
    begin = time.time()
    # detailp.get_filepath()
    print('current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    detailp.up_hdfs()
    print('current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
