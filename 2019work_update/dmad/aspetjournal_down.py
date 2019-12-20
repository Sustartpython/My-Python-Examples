"""
代理:内蒙古大学
proxy:http://192.168.30.176:8243
url:http://aspetjournals.org/
url里包含4本期刊分别是：
期刊1：Drug Metabolism and Disposition
url1：http://dmd.aspetjournals.org/
年份：http://dmd.aspetjournals.org/content/by/year (1973-2019)

期刊2：The Journal of Pharmacology and Experimental Therapeutics
url2：http://jpet.aspetjournals.org/
年份：http://jpet.aspetjournals.org/content/by/year (1909-2020)

期刊3：Molecular Pharmacology
url3：http://molpharm.aspetjournals.org/
年份：http://molpharm.aspetjournals.org/content/by/year (1965-2020)

期刊4：Pharmacological Reviews
url4：http://pharmrev.aspetjournals.org/
年份：http://pharmrev.aspetjournals.org/content/by/year (1949-2020)

author:苏鸿
"""
import os
import re
import time
import datetime
import toml
import pyhdfs
import math
import json
import redis
import random
import pymysql
import utils
import requests
import threading
from threading import Thread,get_ident
from queue import Queue
from parsel import Selector


# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']

year_path = toml.load('config.toml')['year_path']
if not os.path.exists(year_path):
    os.mkdir(year_path)
detail_path = toml.load('config.toml')['detail_path']
if not os.path.exists(detail_path):
    os.mkdir(detail_path)
big_json_path = toml.load('config.toml')['big_json_path']
if not os.path.exists(big_json_path):
    os.mkdir(big_json_path)
merge_big_json_path = toml.load('config.toml')['merge_big_json_path']
if not os.path.exists(merge_big_json_path):
    os.mkdir(merge_big_json_path)

proxy = {
    # 'http' : '192.168.30.176:8243',
    # 'https' : '192.168.30.176:8243',
}

dic_journal = {
    "Drug Metabolism and Disposition" : "http://dmd.aspetjournals.org/content/by/year",
    "The Journal of Pharmacology and Experimental Therapeutics" : "http://jpet.aspetjournals.org/content/by/year",
    "Molecular Pharmacology" : "http://molpharm.aspetjournals.org/content/by/year",
    "Pharmacological Reviews" : "http://pharmrev.aspetjournals.org/content/by/year"
}
new_dict = {v:k for k,v in dic_journal.items()}

redisHost = toml.load('config.toml')['redisHost']
redisPort = toml.load('config.toml')['redisPort']
redisDb = toml.load('config.toml')['redisDb']
connRedis = redis.StrictRedis(host=redisHost, port=redisPort, db=redisDb, decode_responses=True)
RKEY_PROXY = connRedis.keys()
RKEY_PROXY.remove("update_time")
proxieslist = list(RKEY_PROXY)

year_que = Queue()
# 这个队列专门更新数据中stat = 1
sql_queue = Queue()
# 这个队列专门取数据库中stat=0的信息存储
message_que = Queue()

now_time = time.strftime("%Y%m%d")

def get_year2que():
    for year in range(1949,2021):
        base_url = dic_journal['Pharmacological Reviews']
        url = base_url + '/%s' % str(year)
        message = (str(year),url)
        year_que.put(message)
        utils.printf("%s年url添加成功!~" % year)

def down_year():
    get_year2que()
    while True:
        if not year_que.empty():
            message = year_que.get()
            year = message[0]
            url = message[1]
            base_url = url.replace(re.findall("(/\d+)",url)[0],"")
            journal_name = new_dict[base_url]
            feature  = "hw-issue-meta-data"
            res = utils.get_html(url,feature=feature,timeout=30)
            if res:
                res.encoding = res.apparent_encoding
                fdir = year_path + '/' + now_time + '/' + journal_name
                if not os.path.exists(fdir):
                    os.makedirs(fdir)
                fname = '%s/%s.html' % (fdir, year)
                with open(fname,'w',encoding='utf8')as f:
                    f.write(res.text)
                utils.printf("下载%s年成功" % year)
            else:
                year_que.put(message)
        else:
            break

def parse_year():
    fdir = year_path + '/' + now_time
    sql = """
        insert ignore into vol (journal_name,pub_year,vol,num,vol_url) values(%s,%s,%s,%s,%s)
    """
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    for _,fname in utils.file_list(fdir):
        utils.printf(fname)
        journal_name = fname.replace(fdir,"").replace(_,"").replace("\\","")
        with open(fname,encoding='utf-8')as fp:
            text = fp.read()
        html = Selector(text,'html')
        vol_urls = html.xpath("//a[@class='hw-issue-meta-data']/@href").extract()
        for i,item in enumerate(vol_urls):
            base_url = dic_journal[journal_name].replace("/content/by/year","")
            vol_url = base_url + item
            pub_year = _.replace(".html","")
            vol = html.xpath("//a[@class='hw-issue-meta-data']/span[2]/text()").extract()[i]
            vol = re.findall("(\d+)",vol)[0]
            num = html.xpath("//a[@class='hw-issue-meta-data']/span[3]/text()").extract()[i]
            num = re.findall("Issue\s+(.*)",num)[0].replace(" ","_")
            result.append(
                (journal_name,pub_year,vol,num,vol_url)
            )
        if utils.parse_results_to_sql(conn, sql, result, 100):
            print("插入%s条成功" % len(result))
            result.clear()
    utils.parse_results_to_sql(conn, sql, result)
    print("插入剩下%s条成功" % len(result))
    result.clear()

def down_vol():
    sql_up = "update vol set stat = 1 where vol_url = %s"
    result = []
    while True:
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select journal_name,pub_year,vol,num,vol_url from vol where stat = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for journal_name,pub_year,vol,num,vol_url in rows:
                fdir = detail_path + '\\' + now_time + '\\' + journal_name + '\\' + pub_year + '\\' + vol
                if not os.path.exists(fdir):
                    os.makedirs(fdir)
                feature = "highwire-cite-title"
                res = utils.get_html(vol_url,feature=feature,proxies=proxy,timeout=30)
                if res:
                    fname = '%s/%s.html' % (fdir,num)
                    with open(fname,'w',encoding='utf8')as f:
                        f.write(res.text)
                    utils.printf("下载%s期刊%s年%s卷%s期成功" % (journal_name,pub_year,vol,num))
                    result.append(
                        (vol_url)
                    )
                if utils.parse_results_to_sql(conn, sql_up, result, 50):
                    print("更新%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("更新剩下%s条成功" % len(result))
            result.clear()

def parse_vol():
    fdir = detail_path + '\\' + now_time
    sql = """
        insert ignore into detail (url) values(%s)
    """
    result = []
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    for _,fname in utils.file_list(fdir):
        x = fname.replace(fdir,'').replace(_,'')
        a = r'\\(.*)\\\d{4}\\\d+\\'
        journal_name = re.findall(a,x)[0]
        utils.printf(journal_name)
        with open(fname,encoding='utf-8')as fp:
            text = fp.read()
        html = Selector(text,'html')
        url_list = html.xpath("//div[@class='highwire-cite highwire-cite-highwire-article highwire-citation-jnl-jpet-list-complete clearfix']/a[@class='highwire-cite-linked-title']/@href").extract()
        for i,item in enumerate(url_list):
            base_url = dic_journal[journal_name].replace("/content/by/year","")
            url = base_url + item
            result.append(
                (url)
            )
        utils.parse_results_to_sql(conn, sql, result)
        print("插入剩下%s条成功" % len(result))
        result.clear()

def down_cover():
    provider = 'aspetjournal'
    path = r'E:\work\DMAD\cover'
    if not os.path.exists(path):
        os.mkdir(path)
    pathtxt = '%s/%s_cover_%s.txt' % (path,provider,now_time)
    dic_cover = {
        "dmd" : "http://dmd.aspetjournals.org/current-issue/cover-image-medium/cover.jpg",
        "jpet" : "http://jpet.aspetjournals.org/current-issue/cover-image-medium/cover.jpg",
        "molpharm" : "http://molpharm.aspetjournals.org/current-issue/cover-image-medium/cover.jpg",
        "pharmrev" : "http://pharmrev.aspetjournals.org/current-issue/cover-image-medium/cover.jpg"
    }
    for name,url in dic_cover.items():
        res = utils.get_html(url)
        if res:
            cover_path = '%s/%s.jpg' % (path,name)
            utils.Img2Jpg(res.content,cover_path)
            s = provider + '@' + name + '★' + '/smartlib/' + provider + '/' + name + '.jpg' +'\n'
            with open (pathtxt, 'a',encoding='utf-8') as f:
                f.write(s)
            utils.printf('下载%s图片成功' % name)

class MessageTheard(threading.Thread):
    def run(self):
        total = 0
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        cur = conn.cursor()
        sql = "select url from detail where stat=0"
        cur.execute(sql)
        while True:
            rows = cur.fetchall()
            if not rows:
                break
            for url in rows:
                message_que.put(url)
            total +=message_que.qsize()
            utils.printf('total:%d' % total)
            break

class WorkTheard(threading.Thread):
    def run(self):
        big_json_name =big_json_path + '/%s_%s_%s.big_json' % (now_time,os.getpid(),get_ident())
        while True:
            if not message_que.empty():
                rows = message_que.get()
                for url in rows:
                    utils.printf(url)
                    key = random.choice(RKEY_PROXY)
                    proxy_ = connRedis.srandmember(key)
                    proxy = {
                        'http': proxy_,
                        'https': proxy_,
                    }
                    feature = "highwire-cite-metadata"
                    feature_2 = "pane-title"
                    # res = utils.get_html(url,feature=feature,proxies=proxy,timeout=200)
                    res = utils.get_html(url,feature=feature,timeout=200)
                    if res:
                        html = res.text.strip()
                        HEADER = {
                        "Accept" : "*/*",
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
                        }
                        try:
                            sumDict = dict()
                            info_url = url + "/tab-article-info"
                            # res_info = requests.get(info_url,headers=HEADER,proxies=proxy,timeout=20)
                            res_info = requests.get(info_url,headers=HEADER,timeout=200)
                            if res_info.status_code == 200:
                                if res_info.text.find(feature_2) > 0:
                                    info_html = res_info.text.strip()
                                    sumDict['provider_url'] = url
                                    sumDict['down_date'] = now_time
                                    sumDict['htmlText'] = html
                                    sumDict['info_htmlText'] = info_html
                                    with open(big_json_name, mode='a', encoding='utf-8') as f:
                                        line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                        f.write(line)
                                    utils.printf(url,'write to big_json')
                                    sql_queue.put(url)
                                else:
                                    utils.printf("not find feee_info")
                                    message_que.put(rows)
                            elif res_info.status_code == 404:
                                sumDict['provider_url'] = url
                                sumDict['down_date'] = now_time
                                sumDict['htmlText'] = html
                                sumDict['info_htmlText'] = ""
                                with open(big_json_name, mode='a', encoding='utf-8') as f:
                                    line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                    f.write(line)
                                utils.printf(url,'write to big_json')
                                sql_queue.put(url)
                            else:
                                message_que.put(rows)
                        except Exception as e:
                            utils.printf(e)
                            message_que.put(rows)
                    else:
                        message_que.put(rows)

class SqlThread(threading.Thread):
    def run(self):
        conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
        sql_up = "update detail set stat = 1 where url = %s"
        result = []
        while True:
            url = sql_queue.get()
            result.append(
                (url)
            )
            utils.printf(result)
            utils.parse_results_to_sql(conn, sql_up, result)
            print("更新%s条成功" % len(result))
            result.clear()

def merge_bigjson():
    src = big_json_path
    dst = merge_big_json_path
    utils.all_2_one(src, dst)
    # uphdfs()

# 上传到hdfs
def uphdfs():
    #链接hdfs
    # 上传每次都要删除big_json 里的内容，不然会重复上传
    client = pyhdfs.HdfsClient(hosts='hadoop2x-04:50070,hadoop2x-05:50070',user_name='suh')
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    # now_time = '20191212'
    year = time.strftime('%Y',time.localtime(time.time()))
    HdfsDir = r'/RawData/aspetjournal/big_json/%s/%s'%(year,now_time)
    if not client.exists(HdfsDir):
            client.mkdirs(HdfsDir)
    print('Before !%s' % client.listdir(HdfsDir))
    for root, dirs, files in os.walk(merge_big_json_path):
        for file in files:
            up_path = HdfsDir + '/' + file
            local_path = root + '/' + file
            big_json_size = os.path.getsize(local_path)
            if big_json_size != 0 :
                # if client.exists(up_path):
                    # client.delete(up_path)
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

if __name__ == '__main__':
    # down_year()
    # parse_year()
    # down_vol()
    # parse_vol()
    # message = MessageTheard()
    # message.start() 
    # for i in range(1):
    #     work = WorkTheard()
    #     work.start()
    # sqlth = SqlThread()
    # sqlth.start()
    # merge_bigjson()
    # uphdfs()
    down_cover()