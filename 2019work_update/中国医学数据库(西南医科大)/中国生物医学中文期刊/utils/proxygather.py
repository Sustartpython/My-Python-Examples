# encoding: utf-8
# author: walker
# date:
# summary:
# 代理采集程序，采集到的有效IP在setting文件夹IP.txt文件里面

import logging
import os
import re
import socket
import sys
import time
# import pymysql
import traceback

import requests
from bs4 import BeautifulSoup

from . import *
from concurrent import futures
from configparser import ConfigParser

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig()

InformAddressPool = set()

Headers = {
    'Accept':
        '*/*',
    'User-Agent':
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)',
}
UserAgent = 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)'


class ProxyGather:
    # 校验单个代理地址

    @staticmethod
    def checkOneProxy(desturl, proxy, feature):
        proxies = {'http': 'http://' + proxy}
        resp = utils.get_html(desturl, feature=feature, proxies=proxies, timeout=5)
        if not resp:
            return None
        return proxy

    # 校验一组代理地址
    @staticmethod
    def checkOneGroup(rawProxyPool, desturl, feature):
        validProxyList = list()  # 有效代理列表

        pool = futures.ThreadPoolExecutor(16)
        futureList = list()

        [futureList.append(pool.submit(ProxyGather.checkOneProxy, desturl, proxy, feature)) for proxy in rawProxyPool]

        for future in futures.as_completed(futureList):
            proxy = future.result()
            if proxy:
                validProxyList.append(proxy)
        return validProxyList

    # 输入代理列表(set/list)，返回有效代理列表
    @staticmethod
    def getValidProxyPool(rawProxyPool, desturl, feature):
        validProxyList = list()  # 有效代理列表

        t = time.time()
        grpNum = 100  # 每组的个数

        cnt = 0
        proxyPool = list()
        for proxy in rawProxyPool:
            cnt += 1
            proxyPool.append(proxy)
            if (cnt > 0) and (cnt % grpNum == 0):
                validProxyList += ProxyGather.checkOneGroup(proxyPool, desturl, feature)
                proxyPool.clear()
                print('*******************************************')
                print(
                    '*** processed: {}, time cost:{:.2f}s, desturl:{}, feature:{}'.format(
                        cnt, time.time() - t, desturl, feature))
                print('*******************************************')
        # 余数
        validProxyList += ProxyGather.checkOneGroup(proxyPool, desturl, feature)
        print('*******************************************')
        print(
            '*** processed: ' + str(cnt) + ', time cost:' + '{:.2f}s'.format(time.time() - t) + ', desturl:' + desturl)
        print('*******************************************')

        print('validProxyList size:' + str(len(validProxyList)))

        return validProxyList

    # 从http://www.kuaidaili.com/api/getproxy获取代理地址
    # num表示采集数量
    @staticmethod
    def getProxyFromKuaidaili(num):
        url = 'http://www.kuaidaili.com/api/getproxy?'
        url += 'orderid=' + '967796358791160'
        url += '&num=' + str(num)
        # url += '&an_ha=1'       #高匿
        # url += '&quality=0'     #0: 不筛选(默认) 1: VIP功能稳定 2: SVIP功能非常稳定
        # url += '&browser=1'     #0: 支持User-Agent为空; 1: 支持PC浏览器的User-Agent; 2: 支持手机浏览器的User-Agent
        # url += '&protocol=1'    #1: HTTP, 2: HTTPS
        # url += '&method=1'        #1: 支持HTTP GET, 2: 支持 HTTP POST
        # url += '&dedup=1'       #过滤今天提取过的IP

        print('kuaidaili url:' + url)
        ProxyPoolTotal = set()
        r = utils.get_html(url, timeout=10)
        if not r:
            return ProxyPoolTotal  #出现错误直接返回

        # print('kuaidaili:' + repr(r.content))
        lst = r.text.split('\n')
        for line in lst:
            line = line.strip()
            if line.count('.') == 3:
                # print('kuaidaili line:' + line)
                ProxyPoolTotal.add(line)

        return ProxyPoolTotal

    # 从http://proxy.mimvp.com/获取代理地址
    @staticmethod
    def getProxyFromMimvp(num):
        # http://proxy.mimvp.com/api/fetch.php?orderid=860160414114016557&num=100&http_type=1&anonymous=3&ping_time=1&transfer_time=5
        url = 'http://proxy.mimvp.com/api/fetch.php?'
        url += 'orderid=' + '860161011165812474'
        url += '&num=' + str(num)
        url += '&http_type=1'  # 协议类型（http）
        url += '&anonymous=2,3,5'  # 提取透明，匿名+欺骗，高匿
        url += '&ping_time=1'  # 响应时间
        url += '&transfer_time=5'  # 传输速度

        print('mimvp url:' + url)
        proxyPool = set()

        r = utils.get_html(url, timeout=10)
        if not r:
            return proxyPool

        # print('mimvp:' + repr(r.content))
        lst = r.text.split('\n')
        for line in lst:
            line = line.strip()
            if line.count('.') == 3:
                proxyPool.add(line)

        return proxyPool

    # 从http://www.daili666.com/获取代理地址
    @staticmethod
    def getProxyFromDaili666(num):
        url = 'http://tiqu.daili666.com/ip/?'
        url += 'tid=' + '556923006054759'
        url += '&num=' + str(num)
        # url += '&filter=on'
        url += '&foreign=all'
        url += '&delay=5'  # 延迟时间

        print('daili666 url:' + url)

        ProxyPoolTotal = set()
        r = utils.get_html(url, timeout=10)
        if not r:
            return ProxyPoolTotal

        # print('daili666:' + repr(r.text))
        lst = r.text.split('\n')
        for line in lst:
            line = line.strip()
            if line.count('.') == 3:
                ProxyPoolTotal.add(line)

        return ProxyPoolTotal

    # 从http://www.xici.net.co/nn/获取代理地址
    # pageCnt表示共采集几页
    @staticmethod
    def getProxyFromXICI(pageCnt):
        ProxyPoolTotal = set()
        for i in range(1, pageCnt + 1):
            ProxyPool = ProxyGather.getProxyFromXICIOnePage(i)
            ProxyPoolTotal = ProxyPoolTotal.union(ProxyPool)

        return ProxyPoolTotal

    # 从http://www.xici.net.co/nn/获取代理地址
    # pageNum表示采集第几页
    @staticmethod
    def getProxyFromXICIOnePage(pageNum):
        ProxyPool = set()

        url = 'http://www.xicidaili.com/nn/'
        if pageNum > 1:
            url += str(pageNum)

        print(url)
        r = utils.get_html(url, timeout=10)
        if not r:
            return ProxyPool

        html = r.content.decode('utf-8')

        soup = BeautifulSoup(html, 'html.parser')
        ipTable = soup.find('table', id='ip_list')
        if not ipTable:
            print('Error: not ipTable')
            return set()

        for trTag in ipTable.find_all('tr'):
            lst = list(trTag.find_all('td'))
            if len(lst) != 10:
                continue
            ip = ''.join(lst[1].stripped_strings)
            port = ''.join(lst[2].stripped_strings)
            item = ip + ':' + port
            # print(item)
            if re.match(r'^[\d\.:]+$', item):
                ProxyPool.add(item)

        print('len(ProxyPool):' + str(len(ProxyPool)))
        return ProxyPool

    # 读取数据库中的地址
    @staticmethod
    def readDBProxy():
        proxyPool = set()
        with open(utils.UTILS_PATH + '/IP.txt', encoding="utf8") as f:
            results = f.readlines()
        [proxyPool.add(item.strip()) for item in results]

        return proxyPool


def UpdateProxyPool(DstUrl, Feature):
    ProxyPoolTotal = set()

    proxyPool = ProxyGather.readDBProxy()  # 数据库中的代理地址
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(db):' + repr(proxyPool))
    print('*** proxyPool(db) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromXICI(1)  # 采集1页(xici)
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(xici):' + repr(proxyPool) + '\n')
    print('*** proxyPool(xici) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromMimvp(300)  # mimvp api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(mimvp):' + repr(proxyPool))
    print('*** proxyPool(mimvp) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromDaili666(100)  # daili666 api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(daili666):' + repr(proxyPool))
    print('*** proxyPool(daili666) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromKuaidaili(100)  # kuaidaili api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(kuaidaili):' + repr(proxyPool))
    print('*** proxyPool(kuaidaili) size:' + str(len(proxyPool)) + '\n')

    print('*** ProxyPoolTotal size:' + str(len(ProxyPoolTotal)) + '\n')

    ProxyPoolValid = ProxyPoolTotal  # 先假定所有地址有效

    print('\nCheck for %s, %s ... ' % (DstUrl, Feature))
    ProxyPoolValid = ProxyGather.getValidProxyPool(ProxyPoolValid, DstUrl, Feature)
    print('****** ProxyPoolValid size:' + str(len(ProxyPoolValid)) + ' ******')
    print('Write DataBase ...')
    with open(utils.UTILS_PATH + '/IP.txt', mode='w', encoding='utf8') as f:
        for proxy in ProxyPoolValid:
            f.write(proxy)
            f.write('\n')

    # 通知更新
    ProxyPoolValidSize = len(ProxyPoolValid)
    socketClient = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for addr in InformAddressPool:
        vec = addr.split(':')
        ip = vec[0]
        port = int(vec[1])
        netMsg = 'updateProxyFromDB?' + str(len(ProxyPoolValid))
        print('send to ' + addr + ': ' + netMsg)
        socketClient.sendto(netMsg.encode('utf8'), (ip, port))
    socketClient.close()


def main():
    # ReadConfig()  # 读取配置文件
    if not os.path.exists(utils.UTILS_PATH + "/IP.txt"):
        with open(utils.UTILS_PATH + "/IP.txt", mode='w') as f:
            pass
    sleepSpan = 420
    DstUrl = 'http://www.duxiu.com/login.jsp'
    Feature = '系统登录'
    while True:
        startTime = time.time()  # 开始时刻
        UpdateProxyPool(DstUrl, Feature)
        print('****** update timespan:' + '{:.2f}s'.format(time.time() - startTime) + ' ******')
        print('current time:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        print('Program will sleep %d s \n\n' % sleepSpan)
        time.sleep(sleepSpan)


if __name__ == "__main__":

    main()