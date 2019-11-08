import requests
import pypyodbc
import os
import re
import time
import datetime
from queue import Queue
from threading import Thread,get_ident

class doerr(object):
    def __init__(self):
        self.proxy = {
    "HTTP":"192.168.30.172:9172",
        }
        self.headers = {
                    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
                }
        self.path = r'E:\work\verify_url\new_output.txt'
        self.output = r'E:\work\verify_url\last'
        self.message_queue = Queue()
        self.count = 0

    def get_message(self):
        with open(self.path, mode='r', encoding="utf-8") as fp:
            text = fp.readline()
            while text:
                if "html不包含刊名" in text:
                    self.count +=1
                    self.message_queue.put(text)
                    print(self.count)
                text = fp.readline()

    def get_date(self):
        while True:
            if not self.message_queue.empty():
                text = self.message_queue.get()
                message = text.split("\t")
                journal_id = message[0]
                journal_name = message[1]
                old_web_site = message[2]
                url = "http://" + old_web_site
                print("响应%s的%s网页"%(journal_name,old_web_site))
                now_time = datetime.datetime.now().strftime("%Y%m%d")
                name =self.output + '/%s_%s_%s.txt' % (now_time,os.getpid(),get_ident())
                try:
                    res = requests.get(url=url,headers=self.headers,proxies=self.proxy,timeout=30)
                    res.encoding = res.apparent_encoding
                    html = res.text
                    if len(html) > 500:
                        continue
                    if 'window.location=' in html:
                        pattern = re.compile(r'window.location=(.*?);')
                        result1 = pattern.findall(html)
                        new_url = result1[0].replace("\"","")
                        with open(name,'a',encoding="utf-8",newline="") as f:
                            line = journal_id + '\t' + journal_name + '\t' + old_web_site + '\t' + new_url + "\n"
                            f.write(line)
                    elif "<script language=javascript>    window.location.href=" in html:
                        pattern = re.compile(r'window.location.href=(.*)')
                        result2 = pattern.findall(html)
                        new_url = old_web_site + "/" + result2[0].replace("\'","")
                        with open(name,'a',encoding="utf-8",newline="") as f:
                            line = journal_id + '\t' + journal_name + '\t' + old_web_site + '\t' + new_url + "\n"
                            f.write(line)
                    elif " window.location.href = " in html:
                        pattern = re.compile(r'window.location.href = (.*?);')
                        result3 = pattern.findall(html)
                        new_url = result3[0].replace("\'","")
                        with open(name,'a',encoding="utf-8",newline="") as f:
                            line = journal_id + '\t' + journal_name + '\t' + old_web_site + '\t' + new_url + "\n"
                            f.write(line)
                    elif "window.location.href=" in html:
                        pattern = re.compile(r'window.location.href=(.*)')
                        result4 = pattern.findall(html)
                        new_url = old_web_site + '/' + result4[0].replace("\'","").replace("</script>","")
                        with open(name,'a',encoding="utf-8") as f:
                            line = journal_id + '\t' + journal_name + '\t' + old_web_site + '\t' + new_url + "\n"
                            f.write(line)
                except Exception as e:
                    print(old_web_site)
                    print(e)
                    time.sleep(20)
                    self.message_queue.put(message)
            else:
                break
    
    def main(self):
        self.get_message()
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
    v = doerr()
    v.main()






