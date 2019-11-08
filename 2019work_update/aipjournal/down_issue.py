import os
import sys
import time
import re
import requests
import math
import random
from bs4 import BeautifulSoup
import utils
import traceback
import time
import threading
import queue

Headers = {
    'Accept':
        '*/*',
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
}


def downpage(url, proxy):
    urlsplist = url.split('/')
    index_dir = r'E:\lqx\AIP\issue' + '/' + urlsplist[-3]
    index_file = index_dir + '/' + urlsplist[-2] + '_' + urlsplist[-1] + '.html'
    if os.path.exists(index_file):
        return True
    url = url + '?size=all'
    proxies = {"http": "http://{}".format(proxy), "https": "https://{}".format(proxy)}
    feature = 'Table of Contents'
    try:
        resp = utils.get_html(url, feature=feature, proxies=proxies)
    except:
        # exMsg = '* ' + traceback.format_exc()
        # print(exMsg)
        return -1
    if not resp:
        return -1
    if resp.text.find(feature) < 0:
        return -1
    if resp.text.find('</html>') < 0:
        return -1

    if not os.path.exists(index_dir):
        os.makedirs(index_dir)

    with open(index_file, mode='w', encoding='utf8') as f:
        f.write(resp.content.decode('utf8'))
    utils.printf('下载', url, '成功...')
    return True


url_queue = queue.Queue()
sql_queue = queue.Queue()
proxy_queue = queue.Queue()


class WorkerThread(threading.Thread):

    def run(self):
        while True:
            url, stat = url_queue.get()
            proxy = proxy_queue.get()
            # proxy = '127.0.0.1:8087'
            if url:
                try:
                    flag = downpage(url, proxy)
                    if flag == -1:
                        url_queue.put((url, stat))
                        continue
                    if flag == True:
                        sql_queue.put((url, 1))
                except:
                    continue


class SqlThread(threading.Thread):

    def run(self):
        conn = utils.init_db('mysql', 'aipjournal')
        cur = conn.cursor()
        sql = "select url,stat from issue where stat=0 limit 1000;"
        time_last = time.time()
        cnt = 0
        while True:
            if url_queue.empty():
                cur.execute(sql)
                rows = cur.fetchall()
                conn.commit()
                if rows:
                    for row in rows:
                        url_queue.put(row)
                elif sql_queue.empty():
                    break
            time_now = time.time()
            if (sql_queue.qsize() > 100) or (time_now - time_last > 60):
                num = sql_queue.qsize()
                while num > 0:
                    url, flag = sql_queue.get()
                    cur.execute("update issue set stat={} where url='{}'".format(flag, url))
                    cnt += 1
                    num -= 1
                conn.commit()
                utils.printf('succssed:%d' % (cnt))
                time_last = time.time()
            time.sleep(1)


class ProxyThread(threading.Thread):

    def _get_proxy(self):
        """从数据里面读取采集到的代理IP"""
        conn = utils.init_db('mysql', 'aipjournal')
        cur = conn.cursor()
        sql = "select proxy from proxy_pool"
        cur.execute(sql)
        proxies = cur.fetchall()
        cur.close()
        conn.close()
        return proxies

    def _proxy_custom(self, queue):
        """一个代理生产队列"""
        proxies = self._get_proxy()
        for proxy in proxies:
            while True:
                if queue.full():
                    continue
                else:
                    queue.put(proxy[0])
                    break

    def run(self):
        """当代理队列中的IP数量小于等于 1 的时候，就重新从数据库里面读取加入到队列当中当中"""
        self._proxy_custom(proxy_queue)
        while True:
            if proxy_queue.qsize() <= 1:
                self._proxy_custom(proxy_queue)


if __name__ == '__main__':
    proxyth = ProxyThread()
    proxyth.start()
    sqlth = SqlThread()
    sqlth.start()
    for i in range(10):
        work = WorkerThread()
        work.start()