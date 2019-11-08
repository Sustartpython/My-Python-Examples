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
from queue import Queue
from parsel import Selector

# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']
path = toml.load('config.toml')['path']
big_json_path = toml.load('config.toml')['big_json_path']
# 链接数据库
conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)

Headers = {
    'Accept':'*/*',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
}
Proxy = {
    'http' : '192.168.30.176:8127'
}
Url_Que = Queue()


def get_url():
    for year in range(1981,2020):
        url = 'http://www.sinomed.ac.cn/lw/basicSearch.do?dbtype=lw&pageNo=1&pageSize=100&change=true&searchword=+%22{}%22%5B%E5%87%BA%E7%89%88%E5%B9%B4%5D'.format(str(year))
        message = (str(year),url)
        Url_Que.put(message)
    print("get_url结束")

def getlist():
    while True:
        if not Url_Que.empty():
            message = Url_Que.get()
            year = message[0]
            url = message[1]
            try:
                res = requests.get(url,proxies=Proxy,headers=Headers,timeout=30)
                res.encoding = res.apparent_encoding
                if res.status_code == 200:
                    html = Selector(res.text,'html')
                    totalnum = html.xpath("//input[@id='itemTotal']/@value").extract_first('')
                    pages = math.ceil(int(totalnum) / 100)
                    for page in range(1,pages+1):
                        url_detail = "http://www.sinomed.ac.cn/lw/basicSearch.do?dbtype=lw&pageNo={}&pageSize=100&change=true&searchword=+%22{}%22%5B%E5%87%BA%E7%89%88%E5%B9%B4%5D".format(str(page),str(year))
                        sql = "insert ignore into list (url,year,page) values (%s,%s,%s)"
                        message = (url_detail,str(year),str(page))
                        cur = conn.cursor()
                        cur.execute(sql,message)
                        conn.commit()
                        print('%s年第%s页添加到数据库中' % (str(year),str(page)))

                else:
                    print("网页返回码不为200")
                    Url_Que.put(message)
            except Exception as e:
                print(e)
                Url_Que.put(message)
        else:
            break

def downlist():
    cur = conn.cursor()
    while True:
        sql = "select url,page,year from list where stat = 0"
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for url,page,year in rows:
                try:
                    res = requests.get(url,proxies=Proxy,headers=Headers,timeout=30)
                    res.encoding = 'utf-8'
                    if res.status_code == 200:
                        now_time = datetime.datetime.now().strftime("%Y%m%d")
                        fname = path + '\\' + now_time + "\\" + str(year)
                        if not os.path.exists(fname):
                            #创建文件
                            os.makedirs(fname)
                        file_path = fname + '/%s.html'%(page)
                        if os.path.exists(file_path):
                            print('已存在')
                        else:
                            with open(file_path,'w',encoding='utf8')as f:
                                f.write(res.text)
                            print('下载%s年的%s页成功' % (str(year),str(page)))
                        sql = "update list set stat = 1 where url = '%s'" % url
                        cur.execute(sql)
                        conn.commit()
                    else:
                        print("网页返回码不为200")
                except Exception as e:
                    print(e)
                
# fname = path + '\\' + str(year)
# file_path = fname + '/%s.html'%(num)

def parselist():
    base_link = 'http://www.sinomed.ac.cn'
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    file_path = path + '\\' + now_time 
    sql = """insert ignore into detail(provider_url,title,author,pub_date,degree,organ,contributor) values(%s,%s,%s,%s,%s,%s,%s)"""
    result = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            file_name = root + '\\' + file
            # print(file_name)
            with open(file_name,encoding='utf-8')as fp:
                text = fp.read()
            html = Selector(text,'html')
            div_all = html.xpath("//div[@class='right-wztxt fL']")
            for div in div_all:
                author = pud_date = degree = organ = contributor = ""
                # 标题
                title = div.xpath(".//h2/span/a/text()").extract_first('')
                count += 1
                # url
                provider_url = base_link + div.xpath(".//h2/span/a/@href").extract_first('')
                for p in div.xpath(".//p").xpath("string(.)").extract():
                    p = p.replace('\n','').replace('\t','').replace(' ','')
                    # 作者
                    if p.startswith("研究生姓名："):
                        author = p.replace("研究生姓名：",'')
                    # 出版时间
                    if p.startswith("出版时间："):
                        pub_date = p.replace("出版时间：",'').replace("-",'')
                    # 授予学位
                    if p.startswith("授予学位："):
                        degree = p.replace("授予学位：",'')
                    # 授予学位单位
                    if p.startswith("授予学位单位："):
                        organ = p.replace("授予学位单位：",'')
                    # 导师
                    if p.startswith("导师："):
                        contributor = p.replace("导师：",'')
                result.append(
                    (provider_url,title,author,pub_date,degree,organ,contributor)
                )
        if utils.parse_results_to_sql(conn, sql, result, 100):
            print("插入%s条成功" % len(result))
            result.clear()
    utils.parse_results_to_sql(conn, sql, result)
    print("全部插入结束")
    # conn.close()


def wirte_bigjson():
    cur = conn.cursor()
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    big_json_filepath = big_json_path + '/' + '%s.big_json' % str(now_time) 
    sql_up = "update detail set stat = 1 where provider_url = %s"
    result = []
    while True:
        sql = "select provider_url,title,author,pub_date,degree,organ,contributor from detail where stat = 0 limit 1000 "
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            break
        else:
            for provider_url,title,author,pub_date,degree,organ,contributor in rows:
                try:
                    rawid = provider_url.replace('http://www.sinomed.ac.cn/lw/detail.do?ui=','')
                    res = requests.get(provider_url,proxies=Proxy,headers=Headers,timeout=50)
                    res.encoding = 'utf-8'
                    feee = 'title-top fL w100'
                    if res.status_code == 200:
                        html = res.text.strip()
                        if html.find(feee) > 0:
                            sumDict = dict()
                            sumDict['rawid'] = rawid
                            sumDict['provider_url'] = provider_url
                            sumDict['title'] = title
                            sumDict['author'] = author
                            sumDict['pub_date'] = pub_date
                            sumDict['degree'] = degree
                            sumDict['organ'] = organ
                            sumDict['contributor'] = contributor
                            sumDict['htmlText'] = html
                            with open(big_json_filepath, mode='a', encoding='utf-8') as f:
                                line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                f.write(line)
                            print("write--%s--to big_json"%title)
                            result.append(
                                (provider_url)
                            )
                            # sql = "update detail set stat = 1 where provider_url = '%s'" % provider_url
                            # cur.execute(sql)
                            # conn.commit()
                            if os.path.getsize(big_json_filepath) // (1024 * 1024 * 1024) >= 2:
                                big_json_filepath = big_json_path + '/' + '%s_%s.big_json' % (str(now_time),random.randrange(111, 999))
                                print("文件大小到2G，切换文件名为%s" % big_json_filepath)
                        else:
                            print('not find feee')
                    else:
                        print("返回码不为200")
                except Exception as e:
                    print(e)
                if utils.parse_results_to_sql(conn, sql_up, result, 100):
                    print("更新%s条成功" % len(result))
                    result.clear()
            utils.parse_results_to_sql(conn, sql_up, result)
            print("全部更新结束")


def uphdfs():
     #链接hdfs
     # 上传每次都要删除big_json 里的内容，不然会重复上传
    client = pyhdfs.HdfsClient(hosts='hadoop2x-04:50070,hadoop2x-05:50070',user_name='suh')
    now_time = datetime.datetime.now().strftime("%Y%m%d")
    year = time.strftime('%Y',time.localtime(time.time()))
    HdfsDir = r'/RawData/SinoMed/boshuo/big_json/%s/%s'%(year,now_time)
    if not client.exists(HdfsDir):
        client.mkdirs(HdfsDir)
    print('Before !%s' % client.listdir(HdfsDir))
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
    # get_url()
    # getlist()
    # downlist()
    # parselist()
    # wirte_bigjson()
    uphdfs()