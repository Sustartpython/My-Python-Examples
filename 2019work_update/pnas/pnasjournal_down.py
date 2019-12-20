"""
代理:内蒙古大学
proxy:http://192.168.30.176:8243
期刊:PNAS（美国科学院院报）
url:http://www.pnas.org/
年份:https://www.pnas.org/content/by/year    (1915-2019)
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
import random
import pymysql
import utils
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

proxy = {
    'http' : '192.168.30.176:8243'
}

year_que = Queue()

now_time = time.strftime("%Y%m%d")

def get_year2que():
    for year in range(1915,2020):
        url = "https://www.pnas.org/content/by/year/%s" % str(year)
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
            feature  = "hw-issue-meta-data"
            res = utils.get_html(url,feature=feature,proxies=proxy,timeout=30)
            if res:
                res.encoding = res.apparent_encoding
                fdir = year_path + '/' + now_time
                if not os.path.exists(fdir):
                    os.mkdir(fdir)
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
        with open(fname,encoding='utf-8')as fp:
            text = fp.read()
        html = Selector(text,'html')
        journal_name = "Proceedings of the National Academy of Sciences"
        vol_urls = html.xpath("//a[@class='hw-issue-meta-data']/@href").extract()
        for i,item in enumerate(vol_urls):
            vol_url = "https://www.pnas.org" + item
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
        sql = "select pub_year,vol,num,vol_url from vol where stat = 0 limit 1000"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for pub_year,vol,num,vol_url in rows:
                fdir = detail_path + '\\' + now_time + '\\' + pub_year + '\\' + vol
                if not os.path.exists(fdir):
                    os.makedirs(fdir)
                feature = "highwire-cite-title"
                res = utils.get_html(vol_url,feature=feature,proxies=proxy,timeout=30)
                if res:
                    fname = '%s/%s.html' % (fdir,num)
                    with open(fname,'w',encoding='utf8')as f:
                        f.write(res.text)
                    utils.printf("下载%s年%s卷%s期成功" % (pub_year,vol,num))
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
        utils.printf(fname)
        with open(fname,encoding='utf-8')as fp:
            text = fp.read()
        html = Selector(text,'html')
        url_list = html.xpath("//div[@class='highwire-cite highwire-cite-highwire-article highwire-citation-pnas-list-complete clearfix']/a[@class='highwire-cite-linked-title']/@href").extract()
        for i,item in enumerate(url_list):
            url = "https://www.pnas.org" + item
            result.append(
                (url)
            )
        utils.parse_results_to_sql(conn, sql, result)
        print("插入剩下%s条成功" % len(result))
        result.clear()

def wirte_bigjson():
    conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
    cur = conn.cursor()
    big_json_filepath = big_json_path + '/' + '%s.big_json' % now_time
    sql_up = "update detail set stat = 1 where url = %s"
    result = []
    while True:
        down_date = now_time
        sql = "select url from detail where stat=0 limit 1000 "
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for url in rows:
                url = url[0]
                feature = "highwire-cite-metadata"
                res = utils.get_html(url,feature=feature,proxies=proxy,timeout=30)
                # res.encoding = 'utf-8'
                if res:
                    html = res.text.strip()
                    sumDict = dict()
                    sumDict['provider_url'] = url
                    sumDict['down_date'] = down_date
                    sumDict['htmlText'] = html
                    with open(big_json_filepath, mode='a', encoding='utf-8') as f:
                        line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                        f.write(line)
                    utils.printf(url,'write to big_json')
                    result.append(
                        (url)
                    )
                    if os.path.getsize(big_json_filepath) // (1024 * 1024 * 1024) >= 2:
                        big_json_filepath = big_json_path + '/' + '%s_%s.big_json' % (str(now_time),random.randrange(111, 999))
                        print("文件大小到2G，切换文件名为%s" % big_json_filepath)
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
    HdfsDir = r'/RawData/pnasjournal/big_json/%s/%s'%(year,now_time)
    print('Before !%s' % client.listdir(HdfsDir))
    if not client.exists(HdfsDir):
            client.mkdirs(HdfsDir)
    for root, dirs, files in os.walk(big_json_path):
        for file in files:
            up_path = HdfsDir + '/' + file
            local_path = root + '/' + file
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

if __name__ == '__main__':
    # down_year()
    # parse_year()
    # down_vol()
    # parse_vol()
    wirte_bigjson()