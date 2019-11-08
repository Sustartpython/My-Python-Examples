import os
import time
import datetime
import toml
import pyhdfs
import math
import json
import random
import pymysql
import requests
import utils
import re
from bs4 import BeautifulSoup
from queue import Queue
from parsel import Selector


Headers = {
    'Accept':'*/*',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
}
Proxy = {
    'http' : '192.168.30.176:8180', #甘肃医科大
    # 'http' : '192.168.30.176:8127' # 西南
}

article_list_que = Queue()

# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']

conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)

# 期刊（A-Z）列表页路径
journal_list_path = toml.load('config.toml')['journal_list_path']
if not os.path.exists(journal_list_path):
    os.makedirs(journal_list_path)
# 最后文章列表页
article_list_path = toml.load('config.toml')['article_list_path']
if not os.path.exists(article_list_path):
    os.makedirs(article_list_path)
# 优化后的文章列表页存储位置
article_list_2_path = toml.load('config.toml')['article_list_2_path']
if not os.path.exists(article_list_2_path):
    os.makedirs(article_list_2_path)
# big_json存储位置
big_json_path = toml.load('config.toml')['big_json_path']
if not os.path.exists(big_json_path):
    os.makedirs(big_json_path)

# 得到期刊列表页(判断页数)，需要期刊名和它的url
def journal_list():
    url = 'http://www.sinomed.ac.cn/en/journalSearch.do?method=py'
    name_list = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    for name in name_list:
        data = {
            'db': 'journal',
            'dbtype': 'en',
            "searchword": "J_PY='{}%'".format(name),
            'atoz': '%s' % name,
            'cmclass': '',
            'searchfield': 'J_PY',
            'pageNo': '1',
            'pageSize': '20',
            'historyid': '0',
            'orderBy': '+e_title'
        }
        while True:
            try:
                res = requests.post(url,data=data,proxies=Proxy,headers=Headers,timeout=50)
                res.encoding = res.apparent_encoding
                future = '出版单位'
                if res.status_code == 200:
                    if res.text.find(future):
                        html = Selector(res.text,'html')
                        totalnum = html.xpath("//div[@class='page']//b/text()").extract_first('')
                        if totalnum != '0':
                            page = math.ceil(int(totalnum) / 1000)
                            for num in range(1,page+1):
                                data = {
                                    'db': 'journal',
                                    'dbtype': 'en',
                                    "searchword": "J_PY='{}%'".format(name),
                                    'atoz': '%s' % name,
                                    'cmclass': '',
                                    'searchfield': 'J_PY',
                                    'pageNo': str(num),
                                    'pageSize': '1000',
                                    'totalPage': '',
                                    'historyid': '0',
                                    'orderBy': '+e_title'
                                }
                                res = requests.post(url,data=data,proxies=Proxy,headers=Headers,timeout=50)
                                res.encoding = res.apparent_encoding
                                journal_list_name = journal_list_path + '/' + name + '_' + str(num) + '.html'
                                with open(journal_list_name,'w',encoding='utf-8')as f:
                                    f.write(res.text)
                                print("写入%s_%s页成功" % (name,str(num)))
                        break
                    else:
                        print("网页无有效标志")
                else:
                   print("------%s开头的列表错误-------" % name) 
            except  Exception as e:
                print(e)
                print("------%s开头的列表错误-------" % name)

def get_info_from_journal_list():
    sql = """
        insert ignore into journal_list (j_id,journal_url, journal_name, journal_issn) values(%s,%s,%s,%s)
    """
    result = []
    for root, dirs, files in os.walk(journal_list_path):
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        for file in files:
            file_path = root + '/' + file
            print(file_path)
            with open(file_path,encoding='utf-8')as fp:
                text = fp.read()
            html = Selector(text,'html')
            journal_url_list = html.xpath("//table[@class='tabel-lab']//tr/td/a/@href").extract()
            for i,item in enumerate(journal_url_list):
                # http://www.sinomed.ac.cn/en/journalSearch.do?method=detail&id=27706&db=journal&dbtype=en
                journal_url = "http://www.sinomed.ac.cn/en/" + item
                j_id = re.findall("detail&id=(.*)&db=journal",item)[0]
                journal_name = html.xpath("//table[@class='tabel-lab']//tr/td/a/text()").extract()[i]
                journal_issn = html.xpath("//table[@class='tabel-lab']//tr/td[2]/span/text()").extract()[i].strip()
                result.append(
                    (j_id,journal_url,journal_name,journal_issn)
                )
                if utils.parse_results_to_sql(conn, sql, result, 1000):
                    print("插入%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql, result)
            print("插入剩下%s条成功" % len(result))
            result.clear()

# get期刊url，得到隐藏的journal_id，并存储相应的期刊信息  (期刊的年份信息为一个post请求)
def get_hidden_id_years():
    sql_up = "insert ignore into journal_info(j_id,journal_name,journal_id,year) values(%s,%s,%s,%s)"
    sql_uu = "update journal_list set stat = 1 where journal_url = %s"
    sql_insert = "insert ignore into journal_definite(j_id,journal_name,language,start_year,end_year,rate,publisher,country,clc_no) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    result = []
    result2_2 = []
    result3 = []
    while True:
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select j_id,journal_url,journal_name from journal_list where stat = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for j_id,journal_url,journal_name in rows:
                try:
                    res = requests.get(journal_url,proxies=Proxy,headers=Headers,timeout=50)
                    res.encoding = res.apparent_encoding
                    feature = 'title-bg'
                    if res.text.find(feature) < 0:
                        print('%s can not find feature' % journal_url)
                    else:
                        html = Selector(res.text,'html')
                        hidden_journal_id = html.xpath("//input[@id='JOURNAL_ID']/@value").extract_first("")
                        print(journal_name)
                        print(journal_url)
                        div_all = html.xpath("//div[@class='msg-cent fL w100']/p")
                        j_name = language = start_year = end_year = rate = publisher = country = clc_no =  ""
                        for p in div_all.xpath("string(.)").extract():
                            if p.startswith("西文刊名："):
                                j_name = p.replace("西文刊名：",'')
                            if p.startswith('语种：'):
                                language = p.replace('语种：','')
                            if p.startswith('创刊年：'):
                                start_year = p.replace('创刊年：','')
                            if p.startswith("终止年："):
                                end_year = p.replace('终止年：','')
                            if p.startswith('出版频率：'):
                                rate = p.replace('出版频率：','')
                            if p.startswith("出版商："):
                                publisher = p.replace("出版商：",'')
                            if p.startswith("出版国："):
                                country = p.replace("出版国：",'')
                            if p.startswith("分类号："):
                                clc_no = p.replace("分类号：",'')
                        result3.append(
                            (j_id,journal_name,language,start_year,end_year,rate,publisher,country,clc_no)
                        )      
                        # 获取期刊年份 post请求
                        year_url = 'http://www.sinomed.ac.cn/en/journalSearch.do?method=hidden'
                        data = {
                            'dbtype': 'en',
                            'db': 'journal',
                            'searchword': 'JID=%s' % hidden_journal_id
                        }
                        years_res = requests.post(year_url,data=data,proxies=Proxy,headers=Headers,timeout=50)
                        if years_res.status_code == 200:
                            fee = '<html>'
                            if years_res.text.find(fee) < 0:
                                print(years_res.text)
                                result2_2.append(
                                        (journal_url)
                                    )
                                journal_years = years_res.text.split(";")
                                for year in journal_years:
                                    if year != "":
                                        result.append(
                                            (j_id,journal_name,hidden_journal_id,year)
                                        )
                            else:
                                print("年份post出错")
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    print("插入%s条journal_info成功" % len(result))
                    result.clear()
                if utils.parse_results_to_sql(conn, sql_uu, result2_2, 100):
                    print("更新%s条journal_list成功" % len(result2_2))
                    result2_2.clear()
                if utils.parse_results_to_sql(conn, sql_insert, result3, 100):
                    print("插入%s条journal_definite成功" % len(result3))
                    result3.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("插入剩下%s条journal_info成功" % len(result))
            result.clear()
            utils.parse_results_to_sql(conn, sql_uu, result2_2)
            print("更新剩下%s条journal_list成功" % len(result2_2))
            result2_2.clear()
            utils.parse_results_to_sql(conn, sql_insert, result3)
            print("插入剩下%s条journal_definite成功" % len(result3))
            result3.clear()

# 根据期刊名、id、年份、全部期 post得到 文章页,(考虑翻页)
def get_article_list():
    sql_up = "update journal_info set stat = 1 where journal_name = %s and year = %s"
    Header = {
        'Accept':'*/*',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
        'Cookie':'JSESSIONID=3BAC5FDEC6D1346B57261606EA184C74.sinomed;'
    }
    url = "http://www.sinomed.ac.cn/en/basicSearch.do"
    # shijian = datetime.datetime.now().strftime("%Y%m%d")
    result = []
    while True:
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select j_id,journal_name,journal_id,year from journal_info where stat = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for j_id,journal_name,journal_id,year in rows:
                data = {
                    'db': 'journal',
                    'dbtype': 'en',
                    'searchword': '("{}"[英文刊名] OR "{}"[期刊号]) and "{}"[出版年]'.format(journal_name,journal_id,year),
                    'jc': '',
                    'change': 'true',
                    'checked': '',
                    'ajax': 'items',
                    'qkn': '%s' % year,
                    'qkq': '全部期',
                    'TA': '%s' % journal_name,
                    'JOURNAL_ID': '%s' % journal_id,
                    'keyword': '',
                    'qkDetail': 'qkDetail',
                    'pageNo': '1',
                    'pageSize': '20'
                }
                print("do -- %s --- %s ... "%(journal_name, year))
                try:
                    res = requests.post(url,data=data,proxies=Proxy,headers=Header,timeout=30)
                    res.encoding = res.apparent_encoding
                    feeee = 'right-wztxt fL'
                    if res.text.find(feeee) < 0:
                        f_e = 'huise'
                        if res.text.find(f_e) < 0:
                            with open('1.html','w')as f:
                                f.write(res.text)
                            print("can not find feeeeee")
                        else:
                            print("无内容")
                            result.append(
                                (journal_name,year)
                            ) 
                    else:
                        soup = BeautifulSoup(res.text, 'lxml')
                        page_nums= soup.select("div.page > div.page")[0].text
                        page_num_20 = re.findall("共(.*)页",page_nums)[0]
                        page_num = math.ceil(20 * int(page_num_20) / 1000)
                        if page_num == 0:
                            print("无页数")
                            result.append(
                                (journal_name,year)
                            )
                        for page in range(1,int(page_num)+1):
                            article_list_que.put(page)
                        while True:
                            if not article_list_que.empty():
                                page = article_list_que.get()
                                data = {
                                    'db': 'journal',
                                    'dbtype': 'en',
                                    'searchword': '("{}"[英文刊名] OR "{}"[期刊号]) and "{}"[出版年]'.format(journal_name,journal_id,year),
                                    'jc': '',
                                    'change': 'true',
                                    'checked': '',
                                    'ajax': 'items',
                                    'qkn': '%s' % year,
                                    'qkq': '全部期',
                                    'TA': '%s' % journal_name,
                                    'JOURNAL_ID': '%s' % journal_id,
                                    'keyword': '',
                                    'qkDetail': 'qkDetail',
                                    'pageNo': str(page),
                                    'pageSize': '1000'
                                }
                                # shijian = datetime.datetime.now().strftime("%Y%m%d")
                                shijian = '20191031'
                                file_path = article_list_2_path + '\\' + shijian + '\\' + j_id + '\\' + str(year)
                                fname = file_path + '/' + str(page) + ".html"
                                if os.path.exists(fname):
                                    print("%s-%s--page-%s exists~!" % (journal_name, year,str(page)))
                                    result.append(
                                            (journal_name,year)
                                        )
                                else:
                                    try:
                                        res = requests.post(url,data=data,proxies=Proxy,headers=Header,timeout=80)
                                        res.encoding = res.apparent_encoding
                                        feeee = 'right-wztxt fL'
                                        if res.text.find(feeee) < 0:
                                            print("can not find feeeeee")
                                            article_list_que.put(page)
                                        else:
                                            shijian = '20191031'
                                            file_path = article_list_2_path + '\\' + shijian + '\\' + j_id + '\\' + str(year)
                                            if not os.path.exists(file_path):
                                                os.makedirs(file_path)
                                            fname = file_path + '/' + str(page) + ".html"
                                            with open(fname,'w',encoding='utf-8')as f:
                                                f.write(res.text)
                                            times = datetime.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")
                                            print("%s-%s_%s--%s--page-%s down right" % (times, j_id, journal_name, year, str(page)))
                                            result.append(
                                                (journal_name,year)
                                            )
                                    except Exception as e:
                                        print(e)
                                        article_list_que.put(page)
                            else:
                                break
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up,result, 100):
                    print("更新%s条"% len(result))
                    result.clear()
            utils.parse_results_to_sql(conn,sql_up, result)
            print("更新剩下的%s条"% len(result))
            result.clear()
                
# 解析下载的文章列表页，存入数据库
def parse_article_list():
    """
    每次更改时间变量 ，对应下载article_list_2_path文件夹
    """
    sql = """
        insert ignore into article_list(j_id,article_name, article_url) values(%s,%s, %s)
    """
    base_url = "http://www.sinomed.ac.cn"
    result = []
    # shijian = '20191031'
    # file_p = article_list_2_path + '\\' + shijian
    
    for root, dirs, files in os.walk(article_list_path):
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        for file in files:
            try:
                file_path = root + '/' + file
                print(file_path)
                # j_id = re.findall(r'E:\\down_data_e\\sinomed\\en\\article_list_2\\20191031\\(.*)\\',file_path)[0]
                j_id = re.findall(r'E:\\down_data_e\\sinomed\\en\\article_list\\(.*)\\',file_path)[0]
                with open(file_path,encoding='utf-8')as fp:
                    text = fp.read()
                soup = BeautifulSoup(text, 'lxml')
                article_url_list = soup.select('div.right-wztxt.fL > h2 > div > a')
                for atag in article_url_list:
                    url = base_url + atag['href']
                    article_name = atag.text
                    result.append(
                        (j_id,article_name,url)
                    )
                    if utils.parse_results_to_sql(conn, sql, result, 1000):
                        print("插入详情页%s条" % len(result))
                        result.clear()
                utils.parse_results_to_sql(conn, sql, result)
                print("插入剩下详情页%s条" % len(result))
                result.clear()
            except Exception as e:
                print(e)
                line = file_path + '\t' + e + '\n'
                with open("log.txt",'a',encoding='utf-8')as f:
                    f.write(line)

# 访问文章url，存入big_json
def wirte_bigjson():
    cur = conn.cursor()
    # 将definite表里的信息以 k-v 存放在一个字典里 ，k为j_id ，v为剩下的信息
    dict_definite = dict()
    sql_de = "select j_id,journal_name,language,start_year,end_year,rate,publisher,country,clc_no from journal_definite"
    cur.execute(sql_de)
    row_de = cur.fetchall()
    for j_id_1,journal_name,language,start_year,end_year,rate,publisher,country,clc_no in row_de:
        dict_definite[j_id_1] = (journal_name,language,start_year,end_year,rate,publisher,country,clc_no)
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    big_json_filepath = big_json_path + '/' + '%s.big_json' % str(now_time) 
    sql_up = "update article_list set stat = 1 where article_url = %s"
    result = []
    while True:
        down_date = datetime.datetime.now().strftime("%Y%m%d")
        sql = "select j_id, article_name, article_url from article_list where stat=0 limit 1000 "
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for j_id, article_name, article_url in rows:
                try:
                    res = requests.get(article_url,proxies=Proxy,headers=Headers,timeout=50)
                    res.encoding = 'utf-8'
                    feee = 'title-top fL w100'
                    if res.status_code == 200:
                        html = res.text.strip()
                        if html.find(feee) > 0:
                            sumDict = dict()
                            sumDict['journal_id'] = j_id
                            journal_name,language,start_year,end_year,rate,publisher,country,clc_no = dict_definite[j_id]
                            sumDict['journal_name'] = journal_name
                            sumDict['language'] = language
                            sumDict['start_year'] = start_year
                            sumDict['end_year'] = end_year
                            sumDict['rate'] = rate
                            sumDict['publisher'] = publisher
                            sumDict['country'] = country
                            sumDict['clc_no'] = clc_no
                            sumDict['provider_url'] = article_url
                            sumDict['title'] = article_name
                            sumDict['down_date'] = down_date
                            sumDict['htmlText'] = html
                            with open(big_json_filepath, mode='a', encoding='utf-8') as f:
                                line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                f.write(line)
                            times = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
                            print("%s--write--%s--to big_json" % (times,article_name))
                            result.append(
                                (article_url)
                            )
                            if os.path.getsize(big_json_filepath) // (1024 * 1024 * 1024) >= 2:
                                big_json_filepath = big_json_path + '/' + '%s_%s.big_json' % (str(now_time),random.randrange(111, 999))
                                print("文件大小到2G，切换文件名为%s" % big_json_filepath)
                        else:
                            print("not find feee")
                    else:
                        print("返回码不为200")
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 50):
                    print("更新%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("更新剩下的%s条成功" % len(result))
            result.clear()

# 上传到hdfs
def uphdfs():
     #链接hdfs
     # 上传每次都要删除big_json 里的内容，不然会重复上传
    client = pyhdfs.HdfsClient(hosts='hadoop2x-04:50070,hadoop2x-05:50070',user_name='suh')
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    year = time.strftime('%Y',time.localtime(time.time()))
    HdfsDir = r'/RawData/SinoMed/en/big_json/%s/%s'%(year,now_time)
    print('Before !%s' % client.listdir(HdfsDir))
    if not client.exists(HdfsDir):
            client.mkdirs(HdfsDir)
    for root, dirs, files in os.walk(big_json_path):
        for file in files:
            up_path = HdfsDir + '/' + file
            local_path = root + '/' + file
            # local_path = r'E:\work\oalib/date.txt'
            big_json_size = os.path.getsize(local_path)
            if big_json_size != 0 :
                if client.exists(up_path):
                    client.delete(up_path)
                client.copy_from_local(local_path,up_path)
                print('After !%s' % client.listdir(HdfsDir))
                msg = '成功上传到%s' % HdfsDir
                msg2weixin(msg)

# 发送消息到企业微信
def msg2weixin(msg):
    Headers = {
        'Accept':
            '*/*',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        }
    corpid = r'wwa7df1454d730c823'
    corpsecret = r'dDAusBg3gK7hKhLfqIRlyp84UDtII6NkMW7s8Wn2wgs'
    url = r'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s' % (corpid, corpsecret)
    count = 0
    while count < 3:
        try:
            r = requests.get(url)
            content = r.content.decode('utf8')
            dic = json.loads(content)
            accessToken = dic['access_token']
            url = r'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s' % accessToken
            form = {"touser": 'suhong', "msgtype": "text", "agentid": 1000015, "text": {"content": msg}, "safe": 0}

            r = requests.post(url=url, data=json.dumps(form), headers=Headers, timeout=30)
            break
        except:
            count += 1
            print('发送消息到企业微信失败')

if __name__ == "__main__":
    # journal_list()
    # get_info_from_journal_list()
    # get_hidden_id_years()
    get_article_list()
    # parse_article_list()
    # wirte_bigjson()
    # uphdfs()
