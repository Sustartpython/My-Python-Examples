import os
import re
import io
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
from PIL import Image
from bs4 import BeautifulSoup
from queue import Queue
from parsel import Selector


Headers = {
    'Accept':'*/*',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
}
Proxy = {
    # 'http' : '192.168.30.176:8180', # 甘肃
    'http' : '192.168.30.176:8127' # 西南
}

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
# big_json存储位置
big_json_path = toml.load('config.toml')['big_json_path']
if not os.path.exists(big_json_path):
    os.makedirs(big_json_path)
# 图片位置
cover_path = toml.load('config.toml')['cover_path']
if not os.path.exists(cover_path):
    os.makedirs(cover_path)

# 得到期刊列表页(判断页数)，需要期刊名和它的url
def journal_list():
    url = 'http://www.sinomed.ac.cn/kp/journalSearch.do?method=py'
    name_list = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    for name in name_list:
        data = {
            'db': 'journal',
            'dbtype': 'kp',
            "searchword": "J_PY='{}%'".format(name),
            'atoz': '%s' % name,
            'cmclass': '',
            'searchfield': 'J_PY',
            'pageNo': '1',
            'pageSize': '1000',
            'historyid': '0'
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
                                    'dbtype': 'kp',
                                    "searchword": "J_PY='{}%'".format(name),
                                    'atoz': '%s' % name,
                                    'cmclass': '',
                                    'searchfield': 'J_PY',
                                    'pageNo': str(num),
                                    'pageSize': '1000',
                                    'totalPage': '',
                                    'historyid': '0'
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
        insert ignore into journal_list (j_id, journal_url, journal_name, journal_place) values(%s,%s,%s,%s)
    """
    result = []
    for root, dirs, files in os.walk(journal_list_path):
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        for file in files:
            file_path = root + '/' + file
            with open(file_path,encoding='utf-8')as fp:
                text = fp.read()
            html = Selector(text,'html')
            journal_url_list = html.xpath("//table[@class='tabel-lab']//tr/td/a/@href").extract()
            for i,item in enumerate(journal_url_list):
                # http://www.sinomed.ac.cn/kp/journalSearch.do?method=detail&id=1&db=journal&dbtype=kp
                j_id = re.findall("detail&id=(.*)&db=journal",item)[0]
                journal_url = "http://www.sinomed.ac.cn/kp/" + item
                journal_name = html.xpath("//table[@class='tabel-lab']//tr/td/a/text()").extract()[i]
                journal_place = html.xpath("//table[@class='tabel-lab']//tr/td/span/text()").extract()[i]
                result.append(
                    (j_id,journal_url,journal_name,journal_place)
                )
            if utils.parse_results_to_sql(conn, sql, result, 100):
                print("插入%s条成功" % len(result))
                result.clear()
        utils.parse_results_to_sql(conn, sql, result)
        print("插入剩下%s条成功" % len(result))
        result.clear()

# get期刊url，得到隐藏的翻页id，并存储相应的期刊信息  (期刊的年份信息为一个post请求)
def get_hidden_id_years():
    sql_up = "insert ignore into journal_info(j_id,journal_name,journal_id,year) values(%s,%s,%s,%s)"
    sql_uu = "update journal_list set stat = 1,cover_url = %s where journal_url = %s"
    sql_insert = "insert ignore into journal_definite(j_id, journal_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
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
                        cover_url = html.xpath("//div[@class='book-img']/img/@src").extract_first("")
                        print(journal_name)
                        print(journal_url)
                        # 处理框架模板，取出期刊具体信息
                        j_name = issn = cn = publisher = email = journal_year = start_year = address = post_card = clc_no = subject_word = ""
                        div_all = html.xpath("//div[@class='msg-cent fL w100']/p")
                        for p in div_all.xpath("string(.)").extract():
                            # print(p)
                            if p.startswith("刊名："):
                                j_name = p.replace("刊名：",'')
                            if p.startswith("ISSN："):
                                issn = p.replace("ISSN：",'')
                            if p.startswith("CN："):
                                cn = p.replace("CN：",'')
                            if p.startswith('出版单位：'):
                                publisher = p.replace("出版单位：",'')
                            if p.startswith("电子邮箱："):
                                email = p.replace("电子邮箱：",'')
                            if p.startswith("编辑部地址："):
                                address = p.replace("编辑部地址：",'')
                            if p.startswith("期/年："):
                                journal_year = p.replace("期/年：",'')
                            if p.startswith("创刊年："):
                                start_year = p.replace("创刊年：",'')
                            if p.startswith("编辑部邮编："):
                                post_card = p.replace("编辑部邮编：",'')
                            if p.startswith("分类号："):
                                clc_no = p.replace("分类号：",'')
                            if p.startswith("主题词："):
                                subject_word = p.replace("主题词：",'')
                        result3.append(
                            (j_id, j_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card)
                        )
                        # # 获取期刊年份 post请求
                        # year_url = 'http://www.sinomed.ac.cn/kp/journalSearch.do?method=hidden'
                        # data = {
                        #     'dbtype': 'kp',
                        #     'db': 'journal',
                        #     'searchword': "TA='%s'" % journal_name
                        # }
                        # years_res = requests.post(year_url,data=data,proxies=Proxy,headers=Headers,timeout=50)
                        # if years_res.status_code == 200:
                        #     fee = '<html>'
                        #     if years_res.text.find(fee) < 0:
                        #         print(years_res.text)
                        #         result2_2.append(
                        #             (cover_url, journal_url)
                        #         )
                        #         journal_years = years_res.text.split(";")
                        #         for year in journal_years:
                        #             if year != "":
                        #                 result.append(
                        #                     (j_id,journal_name,hidden_journal_id,year)
                        #                 )
                        #     else:
                        #         print("年份post出错")
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    print("插入%s条成功" % len(result))
                    result.clear()
                if utils.parse_results_to_sql(conn, sql_uu, result2_2, 100):
                    print("更新%s条成功" % len(result2_2))
                    result2_2.clear()
                if utils.parse_results_to_sql(conn, sql_insert, result3, 100):
                    print("插入%s条成功" % len(result3))
                    result3.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("插入剩下%s条成功" % len(result))
            result.clear()
            utils.parse_results_to_sql(conn, sql_uu, result2_2)
            print("更新剩下%s条成功" % len(result2_2))
            result2_2.clear()
            utils.parse_results_to_sql(conn, sql_insert, result3)
            print("插入剩下%s条成功" % len(result3))
            result3.clear()

# 下载期刊图片
def get_journal_cover():
    sql_up = "update journal_list set stat_cover = 1 where journal_url = %s"
    result = []
    while True:
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select j_id, journal_url, journal_name, cover_url from journal_list where stat_cover = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for j_id, journal_url, journal_name, cover_url in rows:
                print(journal_name)
                if 'journalNameNull' in cover_url:
                    print("图片错误")
                    result.append(
                        (journal_url)
                    )
                elif cover_url == "":
                    print("无图片")
                    result.append(
                        (journal_url)
                    )
                else:
                    try:
                        res = requests.get(cover_url, headers=Headers,timeout=80)
                        if res.status_code == 200:
                            filename = '%s/%s.jpg' % (cover_path, j_id)
                            srcImg = Image.open(io.BytesIO(res.content))
                            dstImg = srcImg.resize((108, 150), Image.ANTIALIAS).convert('RGB')
                            dstImg.save(filename, 'JPEG')
                            print('下载%s-%s成功'%(journal_name,j_id))
                            provider = 'sinomedkpjournal'
                            pathtxt = r'E:\down_data_e\sinomed\kp\sinomedkpjournal_cover_20191028.txt'
                            s = provider + '@' + j_id + '★' + '/smartlib/' + provider + '/' + j_id + '.jpg' +'\n'
                            with open (pathtxt, 'a',encoding='utf-8') as f:
                                f.write(s)
                            result.append(
                                (journal_url)
                            )
                        else:
                            print("%s-的封面%s status_code !=200" % (journal_name, cover_url))
                    except Exception as e:
                        print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    print("更新%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("更新剩下%s条成功" % len(result))
            result.clear()

# 根据期刊名、id、年份、全部期 post得到 文章页,(考虑翻页)   存储路径改为 + id_journal_name + 年 + html
def get_article_list():
    sql_up = "update journal_info set stat = 1 where journal_name = %s and year = %s"
    Header = {
        'Accept':'*/*',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
        'Cookie':'JSESSIONID=BE9E6956EC6B8A1AED9EF385C698B248.sinomed;'
    }
    url = "http://www.sinomed.ac.cn/kp/basicSearch.do"
    result = []
    while True:
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select journal_name,journal_id,year from journal_info where stat = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for journal_name, journal_id, year in rows:
                data = {
                    'db': 'journal',
                    'dbtype': 'kp',
                    'searchword': '"{}"[刊名] and "{}"[出版年]'.format(journal_name, year),
                    'jc': '%s' % journal_id,
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
                    'pageSize': '20000'
                }
                try:
                    res = requests.post(url,data=data,proxies=Proxy,headers=Header,timeout=80)
                    res.encoding = res.apparent_encoding
                    feeee = 'right-wztxt fL'
                    if res.text.find(feeee) < 0:
                        print("can not find feeeeee")
                    else:
                        file_path = article_list_path + '\\' + journal_id + '\\' + str(year)
                        if not os.path.exists(file_path):
                            os.makedirs(file_path)
                        fname = file_path + '/' + 'all.html'
                        with open(fname,'w',encoding='utf-8')as f:
                            f.write(res.text)
                        times = datetime.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")
                        print("%s---%s的id为%s的%s年down right" % (times,journal_name, j_id, year))
                        result.append(
                            (journal_name,year)
                        )
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
    sql = """
        insert ignore into article_list(j_id, journal_name, article_name, article_url) values(%s, %s, %s, %s)
    """
    base_url = "http://www.sinomed.ac.cn"
    result = []
    for root, dirs, files in os.walk(article_list_path):
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        for file in files:
            file_path = root + '/' + file
            print(file_path)
            id_name = re.findall(r'E:\\down_data_e\\sinomed\\kp\\article_list\\(.*)\\',file_path)[0]
            j_id = id_name.split('_')[0]
            journal_name = id_name.split('_')[1]
            with open(file_path,encoding='utf-8')as fp:
                text = fp.read()
            soup = BeautifulSoup(text, 'lxml')
            article_url_list = soup.select('div.right-wztxt.fL > h2 > div > a')
            for atag in article_url_list:
                url = base_url + atag['href']
                article_name = atag.text
                result.append(
                    (j_id, journal_name, article_name, url)
                )
                if utils.parse_results_to_sql(conn, sql, result, 100):
                    print("插入详情页%s条" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql, result)
            print("插入剩下详情页%s条" % len(result))
            result.clear()

# 访问文章url，存入big_json
def wirte_bigjson():
    cur = conn.cursor()
    # 将definite表里的信息以 k-v 存放在一个字典里 ，k为j_id ，v为剩下的信息
    dict_definite = dict()
    sql_de = 'select j_id, journal_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card from journal_definite'
    cur.execute(sql_de)
    row_de = cur.fetchall()
    for j_id, journal_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card in row_de:
        dict_definite[j_id] = (journal_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card)
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    big_json_filepath = big_json_path + '/' + '%s.big_json' % str(now_time) 
    sql_up = "update article_list set stat = 1 where article_url = %s"
    result = []
    while True:
        down_date = datetime.datetime.now().strftime("%Y%m%d")
        sql = "select j_id, journal_name, article_name, article_url from article_list where stat=0 limit 1000 "
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for j_id, journal_name, article_name, article_url in rows:
                try:
                    res = requests.get(article_url,proxies=Proxy,headers=Headers,timeout=50)
                    res.encoding = 'utf-8'
                    feee = 'title-top fL w100'
                    if res.status_code == 200:
                        html = res.text.strip()
                        if html.find(feee) > 0:
                            sumDict = dict()
                            sumDict['journal_id'] = j_id
                            journal_name, journal_year, start_year, publisher, address, issn, cn, clc_no, subject_word, email, post_card = dict_definite[j_id]
                            sumDict['journal_name'] = journal_name
                            sumDict['journal_year'] = journal_year
                            sumDict['start_year'] = start_year
                            sumDict['publisher'] = publisher
                            sumDict['address'] = address
                            sumDict['issn'] = issn
                            sumDict['cn'] = cn
                            sumDict['clc_no'] = clc_no
                            sumDict['subject_word'] = subject_word
                            sumDict['email'] = email
                            sumDict['post_card'] = post_card
                            sumDict['article_name'] = article_name
                            sumDict['article_url'] = article_url
                            sumDict['htmlText'] = html
                            with open(big_json_filepath, mode='a', encoding='utf-8') as f:
                                line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                f.write(line)
                            times = datetime.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")
                            print("%s---write--%s--to big_json" % (times,article_name))
                            result.append(
                                (article_url)
                            )
                            if os.path.getsize(big_json_filepath) // (1024 * 1024 * 1024) >= 2:
                                big_json_filepath = big_json_path + '/' + '%s_%s.big_json' % (str(now_time),random.randrange(111, 999))
                                print("文件大小到2G，切换文件名为%s" % big_json_filepath)
                        else:
                            print("not find feeee")
                    else:
                        print("返回码不为200")
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
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
    HdfsDir = r'/RawData/SinoMed/kepu/big_json/%s/%s'%(year,now_time)
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

def raname_pp():
    # for root, dirs, files in os.walk(article_list_path):
    #     for file in files:
    #         file_path = root + '/' + file
    #         print(file_path)
    #         id_name = re.findall(r'E:\\down_data_e\\sinomed\\kp\\article_list\\(.*)\\',file_path)[0]
    #         j_id = id_name.split('_')[0]
    #         journal_name = id_name.split('_')[1]
    #         print("%s---%s" % (j_id, journal_name))
    article_list_path = r'E:\down_data_e\sinomed\kp\article_list'
    files = os.listdir(article_list_path)
    count = 0
    for file_name in files:
        # file_name 为文件夹名字 更改为id
        # print(file)
        
        j_id = file_name
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select journal_name from journal_list where j_id = %s"
        cur.execute(sql,file_name)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            journal_name = rows[0][0]
            # print("%s----%s"%(file_name,rows[0][0]))
            old_path = os.path.join(article_list_path,file_name)
            new_file_name = j_id + '_' + journal_name
            new_path = os.path.join(article_list_path,new_file_name)
            os.rename(old_path,new_path)

def q():
    files = os.listdir(cover_path)
    for file_name in files:
        print(file_name)
        j_id = file_name.replace(".jpg",'')
        provider = 'sinomedkpjournal'
        pathtxt = r'E:\down_data_e\sinomed\kp\sinomedkpjournal_cover_20191028.txt'
        s = provider + '@' + j_id + '★' + '/smartlib/' + provider + '/' + j_id + '.jpg' +'\n'
        with open (pathtxt, 'a',encoding='utf-8') as f:
            f.write(s)
     

if __name__ == "__main__":
    # journal_list()
    # get_info_from_journal_list()
    # get_hidden_id_years()
    # get_article_list()
    # parse_article_list()
    # wirte_bigjson()
    # uphdfs()
    # raname_pp()
    # 下载图片
    # get_journal_cover()


