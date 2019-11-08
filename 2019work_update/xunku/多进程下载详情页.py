import os
import re
import toml
import math
import time
import redis
import json
import pymysql
import pyhdfs
import requests
import datetime
from queue import Queue
from parsel import Selector
from threading import Thread,get_ident

class down_detail(object):
    def __init__(self):
        r'''
        定义经常使用的初始化量
        '''
        self.url = 'http://app.xunku.org/dwr/call/plaincall/EpaperSearch.getEdtionDetail.dwr'
        self.cookie = 'JSESSIONID=0A43F264F514B69DAF6EF2F7BB7DB308; DWRSESSIONID=HU9q1A*dZLJjpnh8AYCJM3H5dPm'
        self.scriptSessionId = 'HU9q1A*dZLJjpnh8AYCJM3H5dPm/LgH5dPm-vADdTo8vs'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Rerfer':'http://app.xunku.org',
            'Cookie':self.cookie
        }
        #读取企业微信id
        self.user = toml.load('config.toml')['USER']
        #读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        # 读取配置文件获取存储路径
        self.big_jsonPath = toml.load('config.toml')['big_jsonPath']
        if not os.path.exists(self.big_jsonPath):
            os.makedirs(self.big_jsonPath)
        #创建抓取多线程队列
        self.insert_que = Queue()
        # 创建队列(将执行详情页抓取成功的url放在该队列中，启用一个单线程对数据库更新)
        self.update_que = Queue()
        #插入detail队列
        self.in_detail_que = Queue()
        #更新paper_banmian stat=1
        self.up_banmian_que = Queue()
        #链接数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        #创建big_json文件夹
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        os.chdir(self.big_jsonPath)
        self.pwd = os.getcwd()+"\\"+self.now_time
        # 文件路径
        self.big_json_path = os.path.exists(self.pwd)
        # 判断文件是否存在：不存在创建
        if not self.big_json_path:
            os.makedirs(self.pwd)
    
    def do_sql(self,sql):
        r"""
        执行提交语句
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
    
    def read_sql(self,sql):
        r"""
        查询语句
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        sql_data = cur.fetchall()
        return sql_data
    
    def get_info(self):
        r'''
        先从paper_banmian读取每年报纸当stat为0的时候的版面信息（paper_namem,anchor,id,on_time,creat_time）
        '''
        sql = "select paper_name,anchor,id,on_time,ontime_id from paper_banmian where stat = 0 limit 1000"
        paper_info = self.read_sql(sql)
        if paper_info == ():
            print("没有更新的")
        else:
            for info in paper_info:
                self.insert_que.put(info)
    
    def get_data(self):
        r'''
        抓取数据,清洗数据存入big_json
        '''
        while True:
            if not self.insert_que.empty():
                lists = self.insert_que.get()
                paper_name = lists[0]
                paper_banmian_name = lists[1]
                Id = lists[2]
                on_time = lists[3]
                ontime_id = lists[4]
                data = {
                    'callCount':'1',
                    'windowName':'c0-param0',
                    'c0-scriptName':'EpaperSearch',
                    'c0-methodName':'getEdtionDetail',
                    'c0-id':'0',
                    'c0-e1':'string:%s'%Id,
                    'c0-e2':'string:%s'%on_time,
                    'c0-param0':'Object_Object:{edtionID:reference:c0-e1, day:reference:c0-e2}',
                    'batchId':'51',
                    'instanceId':'0',
                    'page':'%2Findex.jsp',
                    'scriptSessionId':self.scriptSessionId
                }
                while True:
                    try:
                        res = requests.post(url=self.url,headers=self.headers,data=data)
                        if res.status_code == 200:
                            pattern = re.compile(r'r.handleCallback\("51","0",.*newslinks:(.*)\}\);')
                            newpapaer_detail =  pattern.findall(res.text)[0]
                            detail_json = newpapaer_detail.replace("anchor:","\"anchor\":").replace("hashCode:","\"hashCode\":").replace("orgUrl:","\"orgUrl\":").replace("url:","\"url\":")
                            detail_jsons = json.loads(detail_json)
                            for detail_info in detail_jsons:
                                down_time = datetime.date.today().strftime('%Y%m%d')
                                date = on_time.replace("-","")
                                title = detail_info['anchor']
                                hashcode = detail_info['hashCode']
                                orgurl = detail_info['orgUrl']
                                url = detail_info['url']
                                #具体文章的链接
                                detail_url = "http://app.xunku.org/modules/epaper/content.jsp?orgurl=%s&hashcode=%s&url=%s&sourceID=2&searchtype=0&date=%s"%(orgurl,hashcode,url,date)
                                while True:
                                    try:
                                        res2 = requests.get(url=detail_url,headers=self.headers)
                                        if res2.status_code == 200:
                                            text = res2.content.decode('utf8').strip()
                                            sumDict = dict()
                                            sumDict['source'] = paper_name
                                            sumDict['url'] = detail_url
                                            sumDict['date_created'] = date
                                            sumDict['html'] = text
                                            big_json_name =self.pwd + '/%s_%s_%s.big_json' % (self.now_time,os.getpid(),get_ident())
                                            with open(big_json_name, mode='a', encoding='utf-8') as f:
                                                line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                                                f.write(line)
                                            print('%s下载成功'%detail_url)
                                            detail_list = [paper_name,paper_banmian_name,title,date,Id,detail_url,down_time]
                                            self.in_detail_que.put(detail_list)
                                            break
                                        else:
                                            print("%s------ip error and retry!" % detail_url)
                                            self.insert_que.put(lists)      
                                    except Exception as e:
                                        err_now  = datetime.datetime.now().strftime('%Y-%m-%d---%H:%M:%S')
                                        err = str(err_now) + '\t' + str(paper_name) + '\t' + str(Id) + '\t' + str(on_time) + '\t' +  str(e) + '\n'
                                        with open('errr.txt','a')as f:
                                            f.write(err)
                                        continue
                            self.up_banmian_que.put(ontime_id)
                            break
                    except Exception as ee:
                        err_now  = datetime.datetime.now().strftime('%Y-%m-%d---%H:%M:%S')
                        err = str(err_now) + '\t' + str(paper_name) + '\t' + str(Id) + '\t' + str(on_time) + '\t' +  str(ee) + '\n'
                        with open('errr2.txt','a')as f:
                            f.write(err)
                            continue
            else:
                break

    def update_sqls(self):
        while True:
            if not self.up_banmian_que.empty():
                ontime_id = self.up_banmian_que.get()
                sql = "update paper_banmian set stat = 1 where ontime_id = '%s'" % ontime_id
                self.do_sql(sql)
            else:
                break


    def do_in_detail(self):
        while True:
            if not self.in_detail_que.empty():
                detail_list = self.in_detail_que.get()
                paper_name = detail_list[0]
                paper_banmian_name = detail_list[1]
                title = detail_list[2]
                date = detail_list[3]
                Id = detail_list[4]
                detail_url = detail_list[5]
                down_time = detail_list[6]
                sql = "insert ignore into detail (paper_name,paper_banmian_name,title,on_time,id,url,down_time) values('%s','%s','%s','%s','%s','%s','%s')" %(paper_name,paper_banmian_name,title,date,Id,detail_url,down_time)
                print(sql)
                cur = self.conn.cursor()
                cur.execute(sql)
                self.conn.commit()
            else:
                break


    def main(self):
        self.get_info()
        while True:
            self.get_info()
            if not self.insert_que.empty():
                t_list = []
                for i in range(50):
                    t = Thread(target=self.get_data)
                    t_list.append(t)
                    t.start()
                for i in t_list:
                    i.join()
                self.update_sqls()
                self.do_in_detail()

            else:
                break

if __name__ == "__main__":
    do = down_detail()
    do.main()







