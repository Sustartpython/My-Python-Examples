import os
import utils
import json
import time
import toml
import re
import pymysql
import sqlite3
import requests
from parsel import Selector


# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']

now_time = time.strftime("%Y%m%d")

index_path = toml.load('config.toml')['index_path']
if not os.path.exists(index_path):
    os.mkdir(index_path)
list_path = toml.load('config.toml')['list_path']
if not os.path.exists(list_path):
    os.mkdir(list_path)
detail_path = toml.load('config.toml')['detail_path']
if not os.path.exists(detail_path):
    os.mkdir(detail_path)
all_detail_list_path = toml.load('config.toml')['all_detail_list_path']
if not os.path.exists(all_detail_list_path):
    os.mkdir(all_detail_list_path)
cover_path = toml.load('config.toml')['cover_path']
if not os.path.exists(cover_path):
    os.mkdir(cover_path)

def down_index():
    url = "http://www.360elib.com:2100/chinese/web/PageList.aspx"
    feature = "g-content"
    res = utils.get_html(url,feature=feature)
    if res:
        fname = "%s/index.html" % (index_path)
        with open(fname,'w',encoding='utf-8')as f:
            f.write(res.text)
        utils.printf("下载首页成功")

def parse_index():
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    sql = """
        insert ignore into catalog (g_name,g_link) values (%s,%s)
    """
    fname = "%s/index.html" % (index_path)
    with open(fname,encoding='utf-8')as f:
        text = f.read()
    html = Selector(text,'html')
    g_link_name_list = html.xpath("//div[@class='g-link']/a/text()").extract()
    g_link_list = html.xpath("//div[@class='g-link']/a/@href").extract()
    for num,g_link in enumerate(g_link_list):
        g_name = g_link_name_list[num]
        result.append(
            (g_name,g_link)
        )
    utils.parse_results_to_sql(conn,sql,result)
    utils.printf("插入%s条分类信息" % len(result))

def down_list():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    cur = conn.cursor()
    sql = "select g_name,g_link from catalog"
    cur.execute(sql)
    rows = cur.fetchall()
    for g_name,g_link in rows:
        info = re.findall("id=(.*)&childid=(.*)",g_link)[0]
        id_ = info[0]
        childid = info[1]
        # 页数post
        url_page = "http://www.360elib.com:2100/chinese/web/PageList.aspx/pageList1_Show"
        json_page = {'page': '1','id': id_,'childid': childid,'strwhere': '','types': ''}
        res = requests.post(url_page,json=json_page)
        text = json.loads(res.text)['d']
        if text == "":
            utils.printf("%s无图书" % g_name)
        else:
            fdir = "%s/%s/%s" % (list_path,now_time,g_name)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            html = Selector(text)
            page = html.xpath("//li/a/text()").extract()[-1]
            utils.printf("%s has page_num:%s" % (g_name,page))
            for page_num in range(1,int(page)+1):
                fname = "%s/%s.html" % (fdir,str(page_num))
                # 内容post
                url_content = "http://www.360elib.com:2100/chinese/web/PageList.aspx/bookList1_Show"
                josn_content = {'page': str(page_num),'id': id_,'childid': childid,'desc': '','strwhere': '','types': ''}
                res_content = requests.post(url_content,json=josn_content)
                content = json.loads(res_content.text)['d']
                with open(fname,'w',encoding='utf-8')as f:
                    f.write(content)
                    utils.printf("%s down success" % fname)

def parse_list():
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    sql = """
        insert ignore into detail (provider_subject,url,cover_url) values (%s,%s,%s)
    """
    fdir = "%s\%s" % (list_path,now_time)
    for _,dir_ in utils.file_list(fdir):
        utils.printf(dir_)
        pa = r"E:\\work\\360elib\\list\\%s\\(.*)\\" % (now_time)
        provider_subject = re.findall(pa,dir_)[0]
        with open(dir_,encoding='utf-8')as f:
            text = f.read()
        html = Selector(text,'html')
        urls = html.xpath("//li/a[@class='g-img']/@href").extract()
        cover_urls = html.xpath("//li/a[@class='g-img']/img/@src").extract()
        for i,item in enumerate(urls):
            url = item
            cover_url = cover_urls[i]
            result.append(
                (provider_subject,url,cover_url)
            )
    utils.parse_results_to_sql(conn,sql,result)
    utils.printf("插入%s条分类信息" % len(result))

def down_detail():
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    cur = conn.cursor()
    sql = "select provider_subject,url from detail where stat = 0 limit 1000"
    sql_up = "update detail set stat = 1 where url = %s"
    while True:
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for provider_subject,url in rows:
                fdir = "%s/%s/%s" % (detail_path,now_time,provider_subject)
                if not os.path.exists(fdir):
                    os.makedirs(fdir)
                rawid = url.replace("/chinese/web/Details.aspx?id=","")
                fname = "%s/%s.html" % (fdir,rawid)
                if os.path.exists(fname):
                    utils.printf("%s 存在" % rawid)
                    result.append(
                        (url)
                    )
                    continue
                feature = "ctl00_ContentPlaceHolder1_lb_name"
                all_url = "http://www.360elib.com:2100" + url
                res = utils.get_html(all_url,feature=feature)
                if res:
                    # print(res.apparent_encoding)
                    with open(fname,'w',encoding='utf-8')as f:
                        f.write(res.content.decode('gb18030'))
                    utils.printf("下载%s 成功" % rawid)
                    result.append(
                        (url)
                    )
                if utils.parse_results_to_sql(conn,sql_up,result,1000):
                    utils.printf("更新%s条" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn,sql_up,result)
            utils.printf("更新剩下%s条" % len(result))
            result.clear()
                
def parse_detail():
    result = []
    conn_db3 = sqlite3.connect("zt_template.db3")
    sql_in = """
    insert into modify_title_info_zt (Lngid, rawid, provider, type, language, country, provider_url, provider_id, cover, batch, title, description, provider_subject, date, date_created, creator, medium , publisher) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    # 基本信息
    language = 'ZH'
    country = 'CN'
    type_ = '1'
    provider = '360elibbook'
    sub_db_id = '258'
    batch = now_time + '00'
    medium = "2"
    date = "1900"
    date_created = date + "0000"
    fdir = "%s\%s" % (detail_path,now_time)
    for _,dir_ in utils.file_list(fdir):
        utils.printf(dir_)
        pa = r"E:\\work\\360elib\\detail\\%s\\(.*)\\" % (now_time)
        provider_subject = re.findall(pa,dir_)[0]
        if provider_subject == 'None':
            provider_subject = ""
        with open(dir_,encoding='utf-8')as f:
            text = f.read()
        html = Selector(text,'html')
        rawid = _.replace(".html","")
        Lngid = utils.GetLngid(sub_db_id, rawid)
        provider_url = provider + '@' + "http://www.360elib.com:2100/chinese/web/Details.aspx?id=%s" % (rawid)
        provider_id = provider + '@' + rawid
        title = html.xpath("//span[@id='ctl00_ContentPlaceHolder1_lb_name']/text()").extract_first()
        creator = html.xpath("//span[@id='ctl00_ContentPlaceHolder1_lb_zz']/text()").extract_first("").replace(", ","").replace(",","").replace("，","").replace("、","")
        publisher = html.xpath("//span[@id='ctl00_ContentPlaceHolder1_lb_cbs']/text()").extract_first("")
        description = html.xpath("//span[@id='ctl00_ContentPlaceHolder1_lb_bookintro']/text()").extract_first("")
        cover_rawid = rawid.lower()
        cover_p = '%s/%s/%s.jpg' % (cover_path,now_time,cover_rawid)
        if os.path.exists(cover_p):
            cover = "/smartlib" + "/" + provider + "/" + cover_rawid + ".jpg"
        else:
            cover = ""
        result.append(
            (Lngid, rawid, provider, type_, language, country, provider_url, provider_id, cover, batch, title, description, provider_subject, date, date_created, creator, medium , publisher)
        )
        if utils.parse_results_to_sql(conn_db3, sql_in, result, 1000):
            utils.printf("插入%s条" % len(result))
            result.clear()
    utils.parse_results_to_sql(conn_db3, sql_in, result)
    utils.printf("插入剩下得%s条" % len(result))
    result.clear()
        

        

# 由于分类检索出来少了 600本，现在对全部页数做一个下载判断
def down_all_detail_list():
    fdir = "%s/%s" % (all_detail_list_path,now_time)
    if not os.path.exists(fdir):
        os.makedirs(fdir)
    for page_num in range(1,824):
        url = "http://www.360elib.com:2100/chinese/web/PageList.aspx/bookList1_Show"
        json_book = {'page': str(page_num),'id': '','childid': '','desc': '','strwhere': '','types': ''}
        res = requests.post(url,json=json_book)
        text = json.loads(res.text)['d']
        fname = "%s/%s.html" % (fdir,str(page_num))
        with open(fname,'w',encoding='utf-8')as f:
            f.write(text)
            utils.printf("%s down success" % fname)

def parse_all_detail():
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    sql = """
        insert ignore into detail (url,cover_url) values (%s,%s)
    """
    fdir = "%s\%s" % (all_detail_list_path,now_time)
    for _,dir_ in utils.file_list(fdir):
        utils.printf(dir_)
        with open(dir_,encoding='utf-8')as f:
            text = f.read()
        html = Selector(text,'html')
        urls = html.xpath("//li/a[@class='g-img']/@href").extract()
        cover_urls = html.xpath("//li/a[@class='g-img']/img/@src").extract()
        for i,item in enumerate(urls):
            url = item
            cover_url = cover_urls[i]
            result.append(
                (url,cover_url)
            )
    utils.parse_results_to_sql(conn,sql,result)
    utils.printf("插入%s条分类信息" % len(result))

def down_cover():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    sql_up = "update detail set cover_stat = 1 where cover_url = %s"
    sql_fail = "update detail set failcount =failcount+1 where cover_url = %s"
    result = []
    result_fail = []
    fdir = cover_path + '/' + now_time
    if not os.path.exists(fdir):
        os.mkdir(fdir)
    while True:
        cur = conn.cursor()
        sql = "select url,cover_url from detail where cover_stat = 0 and failcount < 5 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for url,cover_url in rows:
                rawid = url.replace("/chinese/web/Details.aspx?id=","")
                url = "http://www.360elib.com:2100" + cover_url
                res = utils.get_html(url)
                if res:
                    new_id = rawid.lower()
                    fname = "%s/%s.jpg" % (fdir,new_id)
                    if utils.Img2Jpg(res.content,fname):
                        result.append(
                            (cover_url)
                        )
                        utils.printf("%s封面下载成功" % new_id)
                    else:
                        result_fail.append(
                            (cover_url)
                        )
                        utils.printf("错误")
                else:
                    result_fail.append(
                        (cover_url)
                    )
                    utils.printf("错误")
                if utils.parse_results_to_sql(conn, sql_fail, result_fail,100):
                    print("错误%s条成功" % len(result_fail))
                    result_fail.clear()
                if utils.parse_results_to_sql(conn, sql_up, result,100):
                    print("更新%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("更新剩下%s条成功" % len(result))
            result.clear() 
            utils.parse_results_to_sql(conn, sql_fail, result_fail)
            print("错误剩下%s条成功" % len(result_fail))
            result_fail.clear() 

if __name__ == "__main__":
    # down_index()
    # parse_index()
    # down_list()
    # parse_list()
    # down_all_detail_list()
    # parse_all_detail()
    down_detail()
    # parse_detail()
    # down_cover()