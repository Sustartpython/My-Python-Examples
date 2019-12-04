import os
import re
import math
import redis
import requests
import toml
import random
from parsel import Selector

class aiaa(object):
    def __init__(self):
        r"""
        初始化程序,定义常量
        """
        self.url = 'https://arc.aiaa.org/action/showPublications?pubType=meetingProc&pageSize=100'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb,decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_aip'))
        # 读取配置文件获取存储路径
        self.indexPath = toml.load('config.toml')['indexPath']
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']


    def down_html(self):
        r"""
        下载起始页(第一页)
        """
        proxy = {
            # 'https':random.choice(self.proxieslist)
        }
        fname = self.indexPath + '/0.html'
        if os.path.exists(fname):
            self.parse_html(fname)
        else:
            res = requests.get(url=self.url, headers=self.headers, proxies=proxy, timeout=20)
            if res.status_code == 200:
                with open(fname, mode='w', encoding='utf-8') as f:
                    f.write(res.content.decode('utf8'))
            self.parse_html(fname)




    def parse_html(self,fname):
        r"""
        解析下载的起始页，获取所有的一级界面
        """
        with open(fname, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        result = html.xpath("//h2[@class='search-result__title']/text()").getall()[0]
        page = re.findall("\d+", result)[0]
        totalpage = math.ceil(int(page) / 100)
        for page in range(0,totalpage):
            url = "https://arc.aiaa.org/action/showPublications?pubType=meetingProc&startPage=%s&pageSize=100" % page
            proxy = {
                # 'https': random.choice(self.proxieslist)
            }
            fnames = self.indexPath + '/%s.html' % page
            if os.path.exists(fnames):
                self.down_list(fnames)
                continue
            else:
                resp = requests.get(url, headers=self.headers, proxies=proxy, timeout=20)
                if resp.status_code == 200:
                    with open(fnames, mode='w', encoding='utf-8') as f:
                        f.write(resp.content.decode('utf8'))
                self.down_list(fnames)



    def down_list(self,fnames):
        r"""
        下载二级界面总共的网页
        """
        with open(fnames, encoding='utf8') as f:
            text = f.read()
        index_html = Selector(text, type='html')
        result = index_html.xpath("//h4[@class='search-item__title']/a/@href").getall()
        print('该页%s个网页链接' % len(result))
        for i,item in enumerate(result):
            try:
                name = re.findall('[A-Z].*', item)[0]
            except:
                name = re.findall('10.2514/([a-z].*)', item)[0]
            list_fname = self.listPath + '/%s.html' % name
            proxy = {
                # 'https': random.choice(self.proxieslist)
            }
            list_url = "https://arc.aiaa.org" + item
            print(list_url)
            if os.path.exists(list_fname):
                print('%s,exists!' % list_fname)
                continue
            else:
                try:
                    res = requests.get(list_url,headers=self.headers,proxies=proxy,timeout=120)
                    print(list_fname)
                    print(res.status_code)
                    if res.status_code == 200:
                        with open(list_fname, mode='w', encoding='utf-8') as f:
                            f.write(res.content.decode('utf8'))
                except:
                    print('next time redown')
                    continue

if __name__ == '__main__':
    ai = aiaa()
    ai.down_html()