"""
金图外文--北华大学采集任务
url：http://10.5.23.18:8079/book/sort.aspx?bclass=&keyword=&field=0&page=1
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
from bs4 import BeautifulSoup
import utils

# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']
list_path = toml.load('config.toml')['list_path']
detail_path = toml.load('config.toml')['detail_path']
proxy = {
    'http' : '192.168.30.176:8034'
    }


def down_list():
    base_url = 'http://10.5.23.18:8079/book/sort.aspx?bclass=&keyword=&field=0&page={page}'
    feature = 'rptBook_ctl00_hlnkBookName'
    for page in range(1,586):
        filename = list_path + '/{page}.html'.format(page=page)
        if os.path.exists(filename):
            utils.printf('第{page}页存在'.format(page=page))
            continue
        resp = utils.get_html(base_url.format(page=page), feature=feature, proxies=proxy, timeout=50)
        if not resp:
            continue
        with open(filename, mode='w', encoding='gb18030') as f:
                f.write(resp.content.decode())
        utils.printf("下载第{page}页完成,总共{pages}。".format(page=page, pages=585))

def parse_list():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    regex_bookid = re.compile(r"bookinfo.aspx\?id=(\d+)")
    stmt = 'insert ignore into book (bookid,stat) values(%s,%s)'
    results = []
    for _, filename in utils.file_list(list_path):
        with open(filename, encoding='gb18030') as f:
            text = f.read()
        bookidlist = regex_bookid.findall(text)
        for bookid in bookidlist:
            results.append((bookid,0))
        if utils.parse_results_to_sql(conn, stmt, results, 1000):
            total = len(results)
            results.clear()
            print('插入 ', total, ' 个结果到数据库成功')
    utils.parse_results_to_sql(conn, stmt, results)
    print('插入 ', len(results), ' 个结果到数据库成功')

def down_detail():
    utils.printf("下载详情页开始...")
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    cur = conn.cursor()
    while True:
        cur.execute('select bookid,stat from book where stat=0 limit 10000')
        rows = cur.fetchall()
        conn.commit()
        if len(rows) == 0:
            break
        for bookid, _ in rows:
            print(bookid)
            url = 'http://10.5.23.18:8079/book/bookinfo.aspx?id={}'.format(bookid)
            dir_path = detail_path + '/' + now_time
            dirname = '%s/%s' % (dir_path,bookid[:3])
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            filename = '%s/%s.html' % (dirname,bookid)
            if os.path.exists(filename):
                sql = 'update book set stat=1 where bookid="{}"'.format(bookid)
                cur.execute(sql)
                conn.commit()
                continue
            resp = utils.get_html(url, proxies=proxy)
            if not resp:
                continue
            with open(filename, mode='w', encoding='gb18030') as f:
                f.write(resp.content.decode())
            sql = 'update book set stat=1 where bookid="{}"'.format(bookid)
            cur.execute(sql)
            conn.commit()
            utils.printf("下载", bookid, "成功...")

def parse_detail():
    conn = sqlite3.connect('template.db3')
    language = "EN"
    type = "1"
    medium = "2"
    provider = "mirrorbeihuakingbook"
    country = "US"
    sub_db_id = "217"
    now_time = time.strftime('%Y%m%d')
    batch = now_time + "00"
    stmt = (
        '''insert into modify_title_info_zt(lngid,rawid,title,creator,description,date,date_created,identifier_pisbn,language,country,provider,provider_url,provider_id,type,medium,batch,publisher) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,);'''
        )
    results = []
    cnt=0
    dir_path = detail_path + '/' + now_time
    for file, fullpath in utils.file_list(dir_path):
        with open(fullpath, encoding='gb18030') as fp:
            txt = fp.read()
        title, creator, publishers, date, identifier_pisbn, description = _parse_detail_one(txt)
        # print(title, creator, publishers, date, identifier_pisbn, description)
        date_created = date + '0000'
        basename, _, ext = file.partition(".")
        rawid = basename
        provider_url = provider + "@http://10.5.23.18:8079/book/bookinfo.aspx?id=" + rawid
        provider_id = provider + "@" + rawid
        lngID = utils.GetLngid(sub_db_id, rawid)
        results.append(
            (
                lngID, rawid, title, creator, description, date, date_created, identifier_pisbn, language,
                country, provider, provider_url, provider_id, type, medium, batch, publishers
            )
        )
        if utils.parse_results_to_sql(conn, stmt, results, 1000):
                cnt+=len(results)
                print('%s:%s' % (time.strftime("%Y/%m/%d %X"),cnt))
                results.clear()
    utils.parse_results_to_sql(conn, stmt, results)
    cnt+=len(results)
    print('%s:%s' % (time.strftime("%Y/%m/%d %X"),cnt))
    conn.close()

def _parse_detail_one(detail_txt):
    soup = BeautifulSoup(detail_txt, "lxml")

    title = soup.select_one("#lblBookName").string
    title = title if title else ""

    creator = soup.select_one("#lblAuthorName").string
    creator = creator.replace(", ", ";") if creator else ""

    publishers = soup.select_one("#lblPubName").string
    publishers = publishers if publishers else ""

    date = soup.select_one("#lblSerise").string
    date = date if date else ""
    if date == '0' or date == '':
        date = '1900'

    identifier_pisbn = soup.select_one("#lblIsbn").string
    identifier_pisbn = identifier_pisbn.strip() if identifier_pisbn else ""

    description = soup.select_one("#lblBookIntro").string
    description = description if description else ""

    return title, creator, publishers, date, identifier_pisbn, description

if __name__ == "__main__":
    # down_list()
    # parse_list()
    # down_detail()
    parse_detail()