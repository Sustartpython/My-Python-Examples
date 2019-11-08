import os
import re
import json
import datetime
import time
import toml
import base64
import hashlib
import pymysql
import requests
import utils
import sqlite3
from PIL import Image
from parsel import Selector

class video_spider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Cookie': 'JSESSIONID=3587C09B8CC33D9787A45CCA0A161663;'
        }
        self.proxy = {
            'http':'192.168.30.176:8002' # 重庆警察学院代理
        }
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        # self.now_time = '20191101'
        #读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        #链接数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        # 读取配置文件获取存储路径
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']
        self.coverPath = toml.load('config.toml')['coverPath']
        if not os.path.exists(self.listPath):
            os.makedirs(self.listPath)
        if not os.path.exists(self.detailPath):
            os.makedirs(self.detailPath)
        if not os.path.exists(self.coverPath):
            os.makedirs(self.coverPath)    

    def down_index(self):
        now_path = self.listPath + '\\' + self.now_time
        if not os.path.exists(now_path):
            os.makedirs(now_path)
        for page_num in range(1,604):
            path = now_path + '/%s.html' % str(page_num)
            if os.path.exists(path):
                print(path + "存在")
                continue
            index_url = "http://lib.fifedu.com/toSearchPage.do?searchvalue=&&curpage=%s&&sort=0&&type=1" % str(page_num)
            try:
                res = requests.get(index_url, headers=self.headers, proxies=self.proxy)
                if res.status_code == 200:
                    fee = 'childmenu'
                    if res.text.find(fee) > 0:
                        with open(path, mode='w', encoding='utf-8') as f:
                            f.write(res.content.decode('utf8'))
                        print('下载第%s页成功' % str(page_num))
                    else:
                        print('not find fee')
                        self.down_index()
                else:
                    print('下载第%s页出现ip错误' % str(page_num))
                    self.down_index()
            except Exception as e:
                print(e)
                self.down_index()

    def parse_index(self):
        sql = "insert ignore into list(rawid,title,url,cover_url) values (%s,%s,%s,%s)"
        result = []
        now_path = self.listPath + '\\' + self.now_time
        for root, dirs, files in os.walk(now_path):
            for file in files:
                file_path = root + '/' + file
                with open(file_path, encoding='utf8') as f:
                    text = f.read()
                html = Selector(text, type='html')
                urls = html.xpath("//div[@class='searchlistdiv']/a/@href").getall()
                for i, item in enumerate(urls):
                    url = "http://lib.fifedu.com/" + item
                    rawid  = item.replace('toVideoPage.do?id=','')
                    cover_url = "http://lib.fifedu.com/" + html.xpath("//div[@class='searchlistdiv']/a/img/@src").getall()[i]
                    title =  html.xpath("//div[@class='searchlistdiv']/a/div/text()").getall()[i].strip()
                    if 'u611.png' in cover_url:
                        print('该链接为zip 不插入db3')
                    else:
                        result.append(
                            (rawid,title,url,cover_url)
                        )
                if utils.parse_results_to_sql(self.conn, sql, result, 100):
                    print('插入%s条' % len(result))
                    result.clear()
            utils.parse_results_to_sql(self.conn, sql, result)
            print('插入剩下%s条' % len(result))
            result.clear()

    def down_detail(self):
        now_path = self.detailPath + '\\' + self.now_time
        if not os.path.exists(now_path):
            os.makedirs(now_path)
        sql_up = "update list set stat = 1 where rawid = %s"
        result = []
        while True:
            sql = "select rawid, url, title from list where stat = 0 limit 1000"
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) == 0:
                break
            else:
                for rawid, url, title  in rows:
                    print(title)
                    print(url)
                    path = now_path + '/%s.html' % rawid
                    if os.path.exists(path):
                        result.append(
                            (rawid)
                        )
                        print('%s exists' % path)
                    else:
                        try:
                            res = requests.get(url,headers=self.headers,proxies=self.proxy)
                            fee = 'detailnavbody'
                            if res.status_code == 200:
                                res.encoding = res.apparent_encoding
                                if res.text.find(fee) > 0:
                                    path = now_path + '/%s.html' % rawid
                                    with open(path, mode='w', encoding='utf-8') as f:
                                        f.write(res.content.decode('utf8'))
                                    print('%s -- down right' % title)
                                    result.append(
                                        (rawid)
                                    )
                                else:
                                    print("not find fee ~")
                            else:
                                print('status_code != 200 ~')
                        except Exception as e:
                            print(e)
                    if utils.parse_results_to_sql(self.conn, sql_up, result, 100):
                        print('插入%s条' % len(result))
                        result.clear()
                utils.parse_results_to_sql(self.conn, sql_up, result)
                print('插入剩下%s条' % len(result))
                result.clear()

    def parse_detail(self):
        cover_list = []
        cover_now_path = self.coverPath + '\\' + self.now_time
        for root, dirs, files in os.walk(cover_now_path):
            for file in files:
                rawid = file.replace('.jpg','')
                cover_list.append(rawid)
        print(len(cover_list))
        conn = sqlite3.connect("video.db3")
        now_path = self.detailPath + '\\' + self.now_time
        sub_db_id = '203'
        sub_db = 'DMT'
        provider = 'fifeduvideo'
        type_ = '10'
        language = 'ZH'
        country = 'CN'
        date = '1900'
        date_created = '19000000'
        medium = '2'
        sql = "insert into modify_title_info_zt(Lngid, rawid, provider, type, language, country, provider_url, provider_id, cover, batch, title, description, provider_subject, date, date_created, medium) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        result = []
        for root, dirs, files in os.walk(now_path):
            for file in files:
                rawid = file.replace('.html','')
                Lngid = utils.GetLngid(sub_db_id, rawid)
                provider_url = provider + '@' + "http://lib.fifedu.com/toVideoPage.do?id=%s" % rawid
                provider_id = provider + '@' + rawid
                if rawid in cover_list:
                        cover = "/smartlib" + "/" + provider + "/" + rawid + ".jpg"
                else:
                    cover = ''
                batch = str(self.now_time) + '00'
                file_path = root + '/' + file
                print(file_path)
                with open(file_path, encoding='utf8') as f:
                    text = f.read()
                html = Selector(text, type='html')
                provider_subject = description = title = ''
                provider_subjects = html.xpath("//div[@class='detailnavbody']/a/text()").extract()[1:-1]
                title = html.xpath("//div[@class='detailnavbody']/a/text()").extract()[-1]
                description = html.xpath("//div[@class='tabvalue']/text()").extract_first('').strip()
                for item in provider_subjects:
                    provider_subject += item + ';'
                    provider_subject = provider_subject.replace('在线课程库;','').replace('玩转多语种;','').replace('视听练兵场;','')
                result.append(
                    (Lngid, rawid, provider, type_, language, country, provider_url, provider_id, cover, batch, title, description, provider_subject, date, date_created, medium)
                )
                # if utils.parse_results_to_sql(conn, sql, result, 100):
                #     print("插入%s条" % len(result))
                #     result.clear()
            utils.parse_results_to_sql(conn, sql, result)
            print("插入剩下得%s条" % len(result))
            result.clear()
                
    def down_cover(self):
        now_path = self.coverPath + '\\' + self.now_time
        if not os.path.exists(now_path):
            os.makedirs(now_path)
        sql_up = "update list set cover_stat = 1 where rawid = %s"
        result = []
        while True:
            sql = "select rawid, cover_url, title from list where cover_stat = 0 limit 1000"
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) == 0:
                break
            else:
                for rawid, cover_url, title in rows:
                    path = now_path + '/%s.jpg' % rawid
                    if os.path.exists(path):
                        result.append(
                            (rawid)
                        )
                        print('%s exists' % path)
                    elif 'zhlx.png' in cover_url:
                        result.append(
                            (rawid)
                        )
                        print('%s no ' % path)
                    elif cover_url == 'http://lib.fifedu.com/upload_dir/':
                        result.append(
                            (rawid)
                        )
                        print('%s no ' % path)
                    else:
                        try:
                            res = requests.get(cover_url,headers=self.headers)
                            if res.status_code == 200:
                                path = now_path + '/%s.jpg' % rawid
                                # utils.Img2Jpg(res.content,path)
                                if utils.Img2Jpg(res.content,path):
                                    print('%s -- down cover right' % title)
                                    result.append(
                                        (rawid)
                                    )
                                else:
                                    print('%s -- down cover error' % title)
                            else:
                                print('status_code != 200 ~')
                        except Exception as e:
                            print(e)
                    if utils.parse_results_to_sql(self.conn, sql_up, result, 100):
                        print('插入%s条' % len(result))
                        result.clear()
                utils.parse_results_to_sql(self.conn, sql_up, result)
                print('插入剩下%s条' % len(result))
                result.clear()
                        

if __name__ == "__main__":
    video = video_spider()
    # print("下载index页面")
    # video.down_index()
    # print("解析index页面")
    # video.parse_index()
    # print("下载detail页面")
    # video.down_detail()
    # print("下载detail封面")
    # video.down_cover()
    # print("解析detail")
    video.parse_detail()