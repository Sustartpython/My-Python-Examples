"""
泛在微讲堂--浙江工商职业技术学院采集任务
url：http://www.fzwjt.com/Course?catalogType=1
站点资源数量：3187
"""
import os
import sys
import re
import time
import toml
import datetime
import requests
import pymysql
import sqlite3
from parsel import Selector
import utils

# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']
list_path = toml.load('config.toml')['list_path']
detail_path = toml.load('config.toml')['detail_path']
cover_path = toml.load('config.toml')['cover_path']

proxy = {
    'http' : '192.168.30.176:8159'
    }

def down_index():
    base_url = 'http://www.fzwjt.com/Course?catalogType=1'
    resp = utils.get_html(base_url, proxies=proxy, timeout=50)
    with open('1.html', mode='w', encoding='utf8') as f:
        f.write(resp.content.decode())

def down_list():
    with open('1.html', mode='r', encoding='utf8') as f:
        text = f.read()
    html = Selector(text,'html')
    dl = html.xpath("//li[@id='thA']//dl")
    for item in dl:
        item : Selector
        dt = item.xpath("./dt/a/text()").extract_first('')
        dd = item.xpath("./dd/a/text()").extract()
        dd_url = item.xpath("./dd/a/@href").extract()
        for i,small in enumerate(dd):
            name = dt + ';' + small
            filename = list_path + '/' + name
            if not os.path.exists(filename):
                os.makedirs(filename)
            list_url = "http://www.fzwjt.com" + dd_url[i]
            print(list_url)
            feature = 'PageBar41'
            res = utils.get_html(list_url, proxies=proxy, timeout=50)
            if res.content.decode('utf8').find(feature) == -1:
                file = '%s/%s.html' % (filename,1)
                with open(file, mode='w', encoding='utf8') as f:
                    f.write(res.content.decode())
                utils.printf("下载", name, "成功...")
            else:
                html = Selector(res.text,'html')
                page_num = html.xpath("//span[@class='PageBar41']//em[3]/text()").extract_first('')
                for page in range(1,int(page_num)+1):
                    url = list_url + "&page={page}".format(page=page)
                    res = utils.get_html(url, proxies=proxy, timeout=50)
                    file = '%s/%s.html' % (filename,page)
                    with open(file, mode='w', encoding='utf8') as f:
                        f.write(res.content.decode())
                    utils.printf("下载", name,page, "成功...")

def parse_list():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    regex_videoid = re.compile(r"/Course/Detail/(\d+)")
    stmt = 'insert ignore into video (rawid,detail_url,cover_url,provider_subject) values(%s,%s,%s,%s)'
    results = []
    for _, filename in utils.file_list(list_path):
        provider_subject = filename.replace('E:\work\\fzwjt\list\\','').replace('\\'+_,'')
        # print(provider_subject)
        with open(filename, encoding='utf8') as f:
            text = f.read()
        html = Selector(text,'html')
        detail_url_list = html.xpath("//ul[@class='videoUl']/li/a/@href").extract()
        cover_url_list = html.xpath("//ul[@class='videoUl']/li/a/img/@src").extract()
        J_detail_url_list = html.xpath("//ul[@class='videoUl']/li[@class='J']/a/@href").extract()
        for x in J_detail_url_list:
            detail_url_list.remove(x)
        for i,item in enumerate(detail_url_list):
            detail_url = 'http://www.fzwjt.com' + item
            rawid = regex_videoid.findall(item)
            cover_url = cover_url_list[i]
            results.append(
                (rawid,detail_url,cover_url,provider_subject)
                )
        if utils.parse_results_to_sql(conn, stmt, results, 1000):
            total = len(results)
            results.clear()
            print('插入 ', total, ' 个结果到数据库成功')
    utils.parse_results_to_sql(conn, stmt, results)
    print('插入 ', len(results), ' 个结果到数据库成功')
                           
def down_detail():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    now_time = time.strftime('%Y%m%d')
    sql_up = "update video set stat = 1 where rawid = %s"
    result = []
    while True:
        sql = "select rawid,provider_subject,detail_url from video where stat=0 limit 1000"
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
                break
        else:
            for rawid,provider_subject,detail_url in rows:
                dirpath = detail_path + '/' + now_time + '/' + provider_subject
                if not os.path.exists(dirpath):
                    os.makedirs(dirpath)
                path = '%s/%s.html' % (dirpath,rawid)
                if os.path.exists(path):
                    result.append(
                        (rawid)
                    )
                    continue
                feature = 'cDetailBox'
                res = utils.get_html(detail_url,feature=feature,proxies=proxy,timeout=50)
                if not res:
                    utils.printf("下载", rawid, "无标志位")
                    continue
                else:
                    with open(path, mode='w', encoding='utf8') as f:
                        f.write(res.content.decode())
                        utils.printf("下载", rawid, "成功...")
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    total = len(result)
                    result.clear()
                    print('更新 ', total, ' 个结果到数据库成功')
            utils.parse_results_to_sql(conn, sql_up, result)
            print('更新 ', len(result), ' 个结果到数据库成功') 
    
def parse_detail():
    conn = sqlite3.connect('video.db3')
    cover_list = []
    now_time = time.strftime('%Y%m%d')
    cover_now_path = cover_path + '\\' + now_time
    for root, dirs, files in os.walk(cover_now_path):
        for file in files:
            rawid = file.replace('.jpg','')
            cover_list.append(rawid)
    sub_db_id = '223'
    provider = 'fzwjtvideo'
    type = '10'
    language = 'ZH'
    country = 'CN'
    date = '1900'
    date_created = '19000000'
    medium = '2'
    result = []
    sql = "insert into modify_title_info_zt(Lngid, rawid, provider, type, language, country, provider_url, provider_id, cover, batch, title, description, title_sub, creator, creator_bio, provider_subject, date, date_created, medium) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    for _, filename in utils.file_list(detail_path):
        replace_1 = r'E:\work\fzwjt\detail' + '\\' + now_time + '\\'
        rawid = _.replace(".html",'')
        Lngid = utils.GetLngid(sub_db_id, rawid)
        provider_subject = filename.replace(replace_1,'').replace('\\'+_,'')
        provider_url = provider + '@' + "http://www.fzwjt.com/Course/Detail/%s" % rawid
        provider_id = provider + '@' + rawid
        batch = str(now_time) + '00'
        cover = ''
        if rawid in cover_list:
            cover = "/smartlib" + "/" + provider + "/" + rawid + ".jpg"
        with open(filename, encoding='utf8') as f:
            text = f.read()
        html = Selector(text,'html')
        title_sub = creator_bio = ""
        title = html.xpath("//div[@class='cInfo']/dl[@class='base']/dt/b/text()").extract_first('')
        description = html.xpath("//div[@class='cInfo']/dl[@class='base']/dd[@class='info']/text()").extract_first('')
        title_subs = html.xpath("//ul[@class='courseListL']/li/p/a/text()").extract()
        for item in title_subs:
            title_sub += item + ';'
        title_sub = title_sub[0:-1]
        creator = html.xpath("//ul[@class='courseListL']/li/span[2]/text()").extract_first("")
        if creator == "解说":
            creator = ""
            creator_bio = ""
        else:
            # 取下一层一个url获取讲师简介
            a = "http://www.fzwjt.com" + html.xpath("//ul[@class='courseListL']/li/p/a/@href").extract_first("")
            feature = 'tagB'
            res = utils.get_html(a,feature=feature,proxies=proxy,timeout=50)
            if res:
                html_2 = Selector(res.text,'html')
                creator_bio = html_2.xpath("//div[@class='tagB']/p/text()").extract_first("").replace("&quot;",'').strip()
        utils.printf(title,'write right')
        # utils.printf(title,creator,creator_bio)
        result.append(
                (Lngid, rawid, provider, type, language, country, provider_url, provider_id, cover, batch, title, description, title_sub, creator, creator_bio, provider_subject, date, date_created, medium)
            )
        if utils.parse_results_to_sql(conn, sql, result, 100):
            print("插入%s条" % len(result))
            result.clear()
    utils.parse_results_to_sql(conn, sql, result)
    print("插入剩下得%s条" % len(result))
    result.clear()

def down_cover():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    now_time = time.strftime('%Y%m%d')
    dirpath = cover_path + '/' + now_time
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    sql_up = "update video set cover_stat = 1 where rawid = %s"
    result = []
    while True:
        sql = "select rawid,cover_url from video where cover_stat=0 limit 1000"
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
                break
        else:
            for rawid,cover_url in rows:
                path = dirpath + '/%s.jpg' % rawid
                res = utils.get_html(cover_url,proxies=proxy,timeout=50)
                if res:
                    if os.path.exists(path):
                        result.append(
                            (rawid)
                        )
                        utils.printf("该", rawid, "存在...")
                    else:
                        if utils.Img2Jpg(res.content,path):
                            result.append(
                                (rawid)
                            )
                            utils.printf("下载", rawid, "成功...")
                        else:
                            print('%s -- down cover error' % rawid)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    total = len(result)
                    result.clear()
                    print('更新 ', total, ' 个结果到数据库成功')
            utils.parse_results_to_sql(conn, sql_up, result)
            print('更新 ', len(result), ' 个结果到数据库成功') 

if __name__ == "__main__":
    # down_list()
    # down_list()
    # parse_list()
    # down_cover()
    # down_detail()
    parse_detail()