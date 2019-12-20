"""
代理:内蒙古工业大学  192.168.30.176:8062
美星外文 	图书	http://202.207.22.13:100/
👇
数字图书首页：http://202.207.22.13:100/Soft_Index.asp
日文图书首页：http://202.207.22.13:100/soft_indexja.asp
author：苏鸿
create_time：2019-12-06
"""
import utils
import toml
import time
import os
import re
import pymysql
import sqlite3
from parsel import Selector

# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']
index_path = toml.load('config.toml')['index_path']
if not os.path.exists(index_path):
    os.mkdir(index_path)
list_path = toml.load('config.toml')['list_path']
if not os.path.exists(list_path):
    os.mkdir(list_path)
detail_path = toml.load('config.toml')['detail_path']
if not os.path.exists(detail_path):
    os.mkdir(detail_path)

proxy = {
    "http" : "192.168.30.176:8062"
}


def down_index():
    index_list = ["http://202.207.22.13:100/Soft_Index.asp","http://202.207.22.13:100/soft_indexja.asp"]
    for i,index_url in enumerate(index_list):
        feature = "title_maintxt"
        resp = utils.get_html(index_url,feature=feature,proxies=proxy)
        if resp:
            fname = "%s/%s.html" % (index_path,i)
            with open(fname, mode='w', encoding='gb18030') as f:
                f.write(resp.content.decode("gb18030"))
            utils.printf("下载", fname , "成功...")

def parse_index():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    result = []
    sql_in = "insert ignore into list(provider_subject,url) values (%s,%s)"
    for _,filedir in utils.file_list(index_path):
        with open(filedir , mode='r', encoding='gb18030') as f:
            text = f.read()
        html = Selector(text,'html')
        big_subject = html.xpath("//table[@class='txt_css']//td[2]/a[2]/text()").extract_first()
        list_urls = html.xpath("//table[@class='title_main']//td[@class='title_maintxt'][1]//a/@href").extract()
        for i,item in enumerate(list_urls):
            provider_subject = big_subject + ";" + html.xpath("//table[@class='title_main']//td[@class='title_maintxt'][1]//a/@title").extract()[i]
            url = "http://202.207.22.13:100/" + item
            result.append(
                (provider_subject,url)
            )
        utils.parse_results_to_sql(conn, sql_in, result)
        print('插入', len(result), ' 个结果到数据库成功') 

def down_list():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    sql = "select provider_subject,url from list"
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    if len(rows) == 0:
        utils.printf("err")
    else:
        for provider_subject,url in rows:
            feature = "tdbg_leftall"
            resp = utils.get_html(url,feature=feature,proxies=proxy)
            if resp:
                # 寻找页数
                text_index = resp.content.decode("gb18030")
                html_index = Selector(text_index,'html')
                all_page = html_index.xpath("//tr[@class='tdbg_leftall']//table[@align='center']//strong/text()").extract_first()
                page_number = re.findall("(\d+)",all_page)[0]
                utils.printf(provider_subject,"need down_page:",page_number)
                for page in range(1,int(page_number)+1):
                    list_url = url + "&page=%s" % page
                    fdir= '%s/%s' % (list_path,provider_subject)
                    if not os.path.exists(fdir):
                        os.mkdir(fdir)
                    fname = '%s/%s.html' % (fdir,page)
                    if os.path.exists(fname):
                        utils.printf(fname,"exists")
                        continue
                    res = utils.get_html(list_url,feature=feature,proxies=proxy)
                    if res:
                        with open(fname, mode='w', encoding='gb18030') as f:
                            f.write(res.content.decode("gb18030"))
                        utils.printf("下载", fname , "成功...")

def parse_list():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    result = []
    sql_in = "insert ignore into detail(provider_subject,title,url,add_time,look_time) values (%s,%s,%s,%s,%s)"
    for _,filedir in utils.file_list(list_path):
        # E:\work\美星外文\list\日文图书;随笔\2.html
        utils.printf(filedir)
        regex = r"E:\\work\\美星外文\\list\\(.*?)\\"
        provider_subject = re.findall(regex,filedir)[0]
        with open(filedir , mode='r', encoding='gb18030') as f:
            text = f.read()
        html = Selector(text,'html')
        list_urls = html.xpath("//tr[@class='tdbg_leftall']/td/strong/a/@href").extract()
        for i,item in enumerate(list_urls):
            title = html.xpath("//tr[@class='tdbg_leftall']/td/strong/a/text()").extract()[i].split("  ")[0]
            url = "http://202.207.22.13:100/" + item
            add_time = html.xpath("//tr[@class='tdbg_leftall']/td[3]/text()").extract()[i]
            look_time = html.xpath("//tr[@class='tdbg_leftall']/td[4]/text()").extract()[i]
            result.append(
                (provider_subject,title,url,add_time,look_time)
            )
        utils.parse_results_to_sql(conn, sql_in, result)
        print('插入', len(result), ' 个结果到数据库成功') 
        result.clear()

def parsel_detail():
    now_time = time.strftime('%Y%m%d')
    conn_1 = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    conn_2 = sqlite3.connect('zt_template.db3')
    sub_db_id = '243'
    provider = 'mirrorimutmeixingbook'
    type = '1'
    date = '1900'
    date_created = '19000000'
    medium = '2'
    sql_up = "update detail set stat = 1 where url = %s"
    sql_in = "insert into modify_title_info_zt(Lngid, rawid, provider, type, language, country, provider_url, provider_id, batch, title, creator, provider_subject, date, date_created, medium) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    result_1 = []
    result_2 = []
    while True:
        sql = "select provider_subject,title,url from detail where stat=0 and failcount < 20 limit 1000"
        cur = conn_1.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for provider_subject,title,url in rows:
                utils.printf(url)
                feature = "tdbg_rightall"
                if "Soft_Showja.asp" in url:
                    SoftID = re.findall("SoftID=(.*)",url)[0]
                    rawid = "ja%s" % SoftID 
                else:
                    SoftID = re.findall("SoftID=(.*)",url)[0]
                    rawid = SoftID 
                fdir = '%s/%s' % (detail_path,now_time)
                if not os.path.exists(fdir):
                    os.makedirs(fdir)
                filename = '%s/%s.html' % (fdir,rawid)
                if os.path.exists(filename):
                    continue
                res = utils.get_html(url,feature=feature,proxies=proxy)
                time.sleep(2)
                if res:
                    with open(filename,'w',encoding='gb18030') as f:
                        f.write(res.content.decode("gb18030"))
                    utils.printf(filename)
                    # html = Selector(res.content.decode("gb18030"),"html")
                    # creator = html.xpath("//table[@style='WORD-BREAK: break-all']//tr/td/text()").extract()[0].replace("作者:","")
                    # if creator == "unknow":
                    #     creator = ""
                    # if "Soft_Showja.asp" in url:
                    #     language = "JA"
                    #     country = "JP"
                    #     SoftID = re.findall("SoftID=(.*?)",url)[0]
                    #     rawid = "ja%s" % SoftID 
                    #     Lngid = utils.GetLngid(sub_db_id,rawid)
                    # else:
                    #     language = "EN"
                    #     country = "US"
                    #     SoftID = re.findall("SoftID=(.*)",url)[0]
                    #     rawid = SoftID 
                    #     Lngid = utils.GetLngid(sub_db_id,rawid)
                    # provider_url = provider + '@' + url
                    # provider_id = provider + '@' + rawid
                    # batch = str(now_time) + '00'
                    result_1.append(
                        (url)
                    )
                    # result_2.append(
                    #     (Lngid, rawid, provider, type, language, country, provider_url, provider_id, batch, title, creator, provider_subject, date, date_created, medium)
                    # )
                if utils.parse_results_to_sql(conn_1, sql_up, result_1, 50):
                    utils.printf("更新%s条" % len(result_1))
                    result_1.clear()
                # if utils.parse_results_to_sql(conn_2, sql_in, result_2, 50):
                #     utils.printf("插入%s条" % len(result_2))
                #     result_2.clear()
            utils.parse_results_to_sql(conn_1, sql_up, result_1)
            utils.printf("更新剩下的%s条" % len(result_1))
            result_1.clear()
            # utils.parse_results_to_sql(conn_2, sql_in, result_2)
            # utils.printf("插入剩下的%s条" % len(result_2))
            # result_2.clear()

def parsel_detail_one():
    conn_1 = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    conn_2 = sqlite3.connect('mirrorimutmeixingbook_20191218.db3')
    sub_db_id = '243'
    provider = 'mirrorimutmeixingbook'
    type = '1'
    date = '1900'
    date_created = '19000000'
    medium = '2'
    sql_in = "insert into modify_title_info_zt(Lngid, rawid, provider, type, language, country, provider_url, provider_id, batch, title, creator, provider_subject, date, date_created, medium) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    result_2 = []
    now_time = time.strftime('%Y%m%d')
    fdir = '%s/%s' % (detail_path,now_time)
    for _,filename in utils.file_list(fdir):
        rawid = _.replace(".html","")
        with open(filename,encoding='gb18030')as f:
            text = f.read()
        html = Selector(text,"html")
        creator = html.xpath("//table[@style='WORD-BREAK: break-all']//tr/td/text()").extract()[0].replace("作者:","")
        if creator == "unknow":
            creator = ""
        if "ja" in rawid:
            id_ = rawid.replace('ja','')
            url = "http://202.207.22.13:100/Soft_Showja.asp?SoftID=%s" % id_
            language = "JA"
            country = "JP"
            Lngid = utils.GetLngid(sub_db_id,rawid)
        else:
            language = "EN"
            country = "US"
            url = "http://202.207.22.13:100/Soft_Show.asp?SoftID=%s" % rawid
            Lngid = utils.GetLngid(sub_db_id,rawid)
        sql = "select title,provider_subject from detail where url = '%s'" % url
        cur = conn_1.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        title = rows[0][0].replace("\n",' ')
        provider_subject = rows[0][1].replace("数字图书;",'')
        provider_url = provider + '@' + url
        provider_id = provider + '@' + rawid
        batch = str(now_time) + '00'
        result_2.append(
            (Lngid, rawid, provider, type, language, country, provider_url, provider_id, batch, title, creator, provider_subject, date, date_created, medium)
        )
    utils.parse_results_to_sql(conn_2, sql_in, result_2)
    utils.printf("插入剩下的%s条" % len(result_2))
    result_2.clear()
        

       

if __name__ == "__main__":
    # down_index()
    # parse_index()
    # down_list()
    # parse_list()
    # parsel_detail()
    parsel_detail_one()

