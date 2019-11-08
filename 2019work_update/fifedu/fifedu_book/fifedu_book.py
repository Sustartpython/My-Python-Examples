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
import json
import utils
import sqlite3
from PIL import Image
from parsel import Selector

class book_spider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Cookie': 'JSESSIONID=DF2365F33F7536DA6765D0C5C66321B9;'
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
        for page_num in range(1,168):
            path = now_path + '/%s.html' % str(page_num)
            if os.path.exists(path):
                print(path + "存在")
                continue
            index_url = "http://lib.fifedu.com/toSearchPage.do?searchvalue=&&curpage=%s&&sort=0&&type=7" % str(page_num)
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

    def parse_detail(self):
        cover_list = []
        cover_now_path = self.coverPath + '\\' + self.now_time
        for root, dirs, files in os.walk(cover_now_path):
            for file in files:
                rawid = file.replace('.jpg','')
                cover_list.append(rawid)
        print(len(cover_list))
        conn = sqlite3.connect("book.db3")
        sub_db_id = '202'
        sub_db = 'TS'
        provider = 'fifedubook'
        type_ = '1'
        language = 'ZH'
        country = 'CN'
        date = '1900'
        date_created = '19000000'
        medium = '2'
        result = []
        result_2 = []
        sql_insert = "insert into modify_title_info_zt(title, rawid, Lngid, provider, provider_id, provider_url, cover, batch, description_unit, type, language, country, date, date_created, medium) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sql_up = 'update list set stat = 1 where rawid = %s'
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
                    Lngid = utils.GetLngid(sub_db_id, rawid)
                    provider_url = provider + '@' + "http://lib.fifedu.com/toVideoPage.do?id=%s" % rawid
                    provider_id = provider + '@' + rawid
                    if rawid in cover_list:
                        cover = "/smartlib" + "/" + provider + "/" + rawid + ".jpg"
                    else:
                        cover = ''
                    batch = str(self.now_time) + '00'
                    try:
                        res = requests.get(url,headers=self.headers,proxies=self.proxy)
                        fe = 'iframe'
                        if res.status_code == 200:
                            if res.text.find(fe) > 0:
                                html = Selector(res.text, type='html')
                                mulu_id = html.xpath("//div[@align='center']/iframe/@src").extract_first('').replace('http://www.iyangcong.com/service/ilearning/reading?id=','')
                                mulu_url = 'http://www.iyangcong.com/book/catalog/1/%s' % mulu_id
                                res_mulu = requests.get(mulu_url)
                                if res_mulu.text == 'null':
                                    mulu_url = 'http://www.iyangcong.com/book/catalog/10/%s' % mulu_id
                                    res_mulu = requests.get(mulu_url)
                                mulu_list = json.loads(res_mulu.text)
                                mulu_zh = mulu_en = ""
                                for mulu in mulu_list:
                                    if mulu['title_zh'].replace('<p>','').replace('</p>','').replace('<font color=#003D79>','').replace('</font>','').replace('<center>','').replace('</center>','').replace('[^1]','').replace('<ol type="a">','').replace('<li></li>','').replace('</ol>','').replace('<ol>','').replace('<li>','').replace('</li>','').strip().replace('<u>','').replace('</u>','').replace("</strong>",'').replace('<strong>','').replace('</CENTER>','').replace('<CENTER>','').replace('</h1>','').replace('<h1>','').replace('<font color=#844200>','') !='':
                                        mulu_zh += mulu['title_zh'].replace('<p>','').replace('</p>','').replace('<font color=#003D79>','').replace('</font>','').replace('<center>','').replace('</center>','').replace('[^1]','').replace('<ol type="a">','').replace('<li></li>','').replace('</ol>','').replace('<ol>','').replace('<li>','').replace('</li>','').strip().replace('<u>','').replace('</u>','').replace("</strong>",'').replace('<strong>','').replace('</CENTER>','').replace('<CENTER>','').replace('</h1>','').replace('<h1>','').replace('</h2>','').replace('<h2>','').replace('<font color=#844200>','').replace('<font color=brown>','').replace('<div align="center">','').replace('</div>','')  + ';'
                                    else:
                                        mulu_zh += ''

                                    if mulu['title_en'].replace('<p>','').replace('</p>','').replace('<font color=#003D79>','').replace('</font>','').replace('<center>','').replace('</center>','').replace('[^1]','').replace('<ol type="a">','').replace('<li></li>','').replace('</ol>','').replace('<ol>','').replace('<li>','').replace('</li>','').strip().replace('<u>','').replace('</u>','').replace("</strong>",'').replace('<strong>','').replace('</CENTER>','').replace('<CENTER>','').replace('</h1>','').replace('<h1>','').replace('<font color=#844200>','') != '':
                                        mulu_en += mulu['title_en'].replace('<p>','').replace('</p>','').replace('<font color=#003D79>','').replace('</font>','').replace('<center>','').replace('</center>','').replace('[^1]','').replace('<ol type="a">','').replace('<li></li>','').replace('</ol>','').replace('<ol>','').replace('<li>','').replace('</li>','').strip().replace('<u>','').replace('</u>','').replace("</strong>",'').replace('<strong>','').replace('</CENTER>','').replace('<CENTER>','').replace('</h1>','').replace('<h1>','').replace('</h2>','').replace('<h2>','').replace('<font color=#844200>','').replace('<font color=brown>','').replace('<div align="center">','').replace('</div>','') + ';'
                                    else:
                                        mulu_en += ''
                                if mulu_zh.replace(';','') == '':
                                    description_unit = mulu_en
                                else:
                                    description_unit = mulu_zh
                                # print(description_unit) 
                                result.append(
                                    (title, rawid, Lngid, provider, provider_id, provider_url, cover, batch, description_unit, type_, language, country, date, date_created, medium)
                                )
                                result_2.append(
                                    (rawid)
                                )
                                if utils.parse_results_to_sql(conn, sql_insert, result,100):
                                    print("插入%s条" % len(result))
                                    result.clear()
                                if utils.parse_results_to_sql(self.conn, sql_up, result_2,100):
                                    print("更新%s条" % len(result_2))
                                    result_2.clear()
                            else:
                                print('not find fe')
                    except Exception as e:
                        print(e)
                utils.parse_results_to_sql(conn, sql_insert, result)
                print("插入%s条" % len(result))
                result.clear()
                utils.parse_results_to_sql(self.conn, sql_up, result_2)
                print("更新%s条" % len(result_2))
                result_2.clear()

                     
    def down_cover(self):
        now_path = self.coverPath + '\\' + self.now_time
        if not os.path.exists(now_path):
            os.makedirs(now_path)
        sql_up = "update list set cover_stat = 1 where rawid = %s"
        sql_fail = "update list set fail_count = %s where rawid = %s"
        result = []
        result_fail = []
        while True:
            sql = "select rawid, cover_url, title, fail_count from list where cover_stat = 0 and fail_count < 10 limit 1000 "
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) == 0:
                break
            else:
                for rawid, cover_url, title, fail_count in rows:
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
                                    fail_count += 1
                                    result_fail.append(
                                        (fail_count,rawid)
                                    )
                                    utils.parse_results_to_sql(self.conn, sql_fail, result_fail)
                            else:
                                print('status_code != 200 ~')
                                fail_count += 1
                                result_fail.append(
                                    (fail_count,rawid)
                                )
                                utils.parse_results_to_sql(self.conn, sql_fail, result_fail)
                        except Exception as e:
                            print(e)
                            # pass
                    if utils.parse_results_to_sql(self.conn, sql_up, result, 100):
                        print('插入%s条' % len(result))
                        result.clear()
                utils.parse_results_to_sql(self.conn, sql_up, result)
                print('插入剩下%s条' % len(result))
                result.clear()
                        

if __name__ == "__main__":
    book = book_spider()
    # book.down_index()
    # book.parse_index()
    # book.down_detail()
    book.parse_detail()
    # book.down_cover()