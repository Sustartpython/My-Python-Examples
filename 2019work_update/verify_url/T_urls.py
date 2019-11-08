import requests
import pypyodbc
import os
import datetime
from queue import Queue
from threading import Thread,get_ident

class Verify_Url_Demo(object):
    def __init__(self):
        self.dbpath = r'E:\work\verify_url\中外期刊信息库201900906 - 副本.mdb'
        self.output = r'E:\work\verify_url\many'
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        self.proxy ={
            "HTTP":"192.168.30.172:9172",
        }
        self.count = 0
        self.db = pypyodbc.win_connect_mdb(self.dbpath)
        self.message_queue = Queue()

    
    def ttt(self):
        curser = self.db.cursor()
        sql = "select journal_id,journal_name,web_site from kmxxk where country = 'CN' and web_site is not null and web_site <> ''"
        result = curser.execute(sql).fetchall()
        for i,item in enumerate(result):
            self.count +=1
            journal_id = item[0]
            journal_name = item[1]
            web_site = item[2]
            url = "http://" + web_site
            message = [journal_id,journal_name,web_site,url]
            self.message_queue.put(message)
            print(self.count)
    
    def get_date(self):
        while True:
            if not self.message_queue.empty():
                message = self.message_queue.get()
                journal_id = message[0]
                journal_name = message[1]
                web_site = message[2]
                url = message[3]
                print("测试%s的%s网页"%(journal_name,web_site))
                now_time = datetime.datetime.now().strftime("%Y%m%d")
                name =self.output + '/%s_%s_%s.txt' % (now_time,os.getpid(),get_ident())
                try:
                    res = requests.get(url=url,headers=self.headers,proxies=self.proxy,timeout=30)
                    if res.status_code != 200:
                        with open(name,'a',encoding="utf-8") as f:
                            line = journal_id + '\t' + journal_name + '\t' +web_site + '\t' + "链接失效" + "\n"
                            f.write(line)
                    if res.status_code == 200:
                        res.encoding = res.apparent_encoding
                        html = res.text
                        if journal_name not in html:
                            with open(name,'a',encoding="utf-8") as f:
                                line = journal_id + '\t' + journal_name + '\t' +web_site + '\t' + "html不包含刊名" + "\n"
                                f.write(line)
                except Exception as e:
                    with open(name,'a',encoding="utf-8") as f:
                            line = journal_id + '\t' + journal_name + '\t' +web_site + '\t' + "链接失效" + "\n"
                            f.write(line)
            else:
                break

    def main(self):
        self.ttt()
        # 空列表
        t_list = []
        # 创建多个线程并启动线程
        for i in range(30):
            t = Thread(target=self.get_date)
            t_list.append(t)
            t.start()
        # 回收线程
        for i in t_list:
            i.join()

if __name__ == "__main__":
    v = Verify_Url_Demo()
    v.main()