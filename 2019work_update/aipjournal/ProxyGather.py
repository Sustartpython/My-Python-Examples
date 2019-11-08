# encoding: utf-8
# author: walker
# date:
# summary:


import os, sys, time, logging
import re
import pymysql
import traceback
import socket, requests
from configparser import ConfigParser
from bs4 import BeautifulSoup
from concurrent import futures

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig()

DBHost = ''
DBPort = 0
DBUser = ''
DBPwd = ''
DBName = ''
DstUrl = ''
Feature = ''
InformAddressPool = set()

Headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)',
}
UserAgent = 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)'


class ProxyGather:
    # 校验单个代理地址
    @staticmethod
    def checkOneProxy(desturl, proxy, feature):
        proxies = {'http': 'http://' + proxy}
        proxies = {
            'http': proxy,
            'https': proxy
        }

        r = None  # 声明
        exMsg = None
        try:
            r = requests.get(url=desturl, headers=Headers, proxies=proxies, timeout=5)
        except:
            exMsg = '* ' + traceback.format_exc()
        # print(exMsg)
        finally:
            if 'r' in locals() and r:
                r.close()

        if exMsg:
            return ''

        if r.status_code != 200:
            return ''

        if r.text.find(feature) < 0:
            return ''

        return proxy

    # 校验一组代理地址
    @staticmethod
    def checkOneGroup(rawProxyPool, desturl, feature):
        validProxyList = list()  # 有效代理列表

        pool = futures.ThreadPoolExecutor(16)
        futureList = list()
        for proxy in rawProxyPool:
            futureList.append(pool.submit(ProxyGather.checkOneProxy, desturl, proxy, feature))

        # print('\nrequests done, waiting for responses\n')

        for future in futures.as_completed(futureList):
            proxy = future.result()
            # print('proxy:' + proxy)
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
                print('*** processed: ' + str(cnt) + ', time cost:' + '{:.2f}s'.format(
                    time.time() - t) + ', desturl:' + desturl + ', feature:' + feature)
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
        r = None  # 声明
        exMsg = None
        try:
            # print('before url:' + url)
            r = requests.get(url=url, headers=Headers, timeout=10)
        except:
            exMsg = repr(sys.exc_info()[0]) + ';' + repr(sys.exc_info()[1])
            print('exMsg:' + exMsg)
        finally:
            if 'r' in locals() and r:
                r.close()

        if exMsg or r.status_code != 200:
            return set()

        ProxyPoolTotal = set()

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
        r = None  # 声明
        exMsg = None
        try:
            # print('before url:' + url)
            r = requests.get(url=url, headers=Headers, timeout=10)
        except:
            exMsg = '*' + traceback.format_exc()
            print('exMsg:' + exMsg)
        finally:
            if 'r' in locals() and r:
                r.close()

        if exMsg or r.status_code != 200:
            return set()

        proxyPool = set()

        # print('mimvp:' + repr(r.content))
        lst = r.text.split('\n')
        for line in lst:
            line = line.strip()
            if line.count('.') == 3:
                proxyPool.add(line)

        # print('mimvp proxyPool:' + repr(proxyPool))
        # print('mimvp proxyPool size:' + str(len(proxyPool)))

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
        r = None  # 声明
        exMsg = None
        try:
            # print('before url:' + url)
            r = requests.get(url=url, headers=Headers, timeout=10)
        except:
            exMsg = '*' + traceback.format_exc()
            print('exMsg:' + exMsg)
        finally:
            if 'r' in locals() and r:
                r.close()

        if exMsg or r.status_code != 200:
            return set()

        ProxyPoolTotal = set()

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

        # print('ProxyPoolTotal:' + repr(ProxyPoolTotal))
        # print('ProxyPoolTotal size:' + str(len(ProxyPoolTotal)))

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

        r = None  # 声明
        exMsg = None
        try:
            # print('before url:' + url)
            r = requests.get(url=url, headers=Headers, timeout=10)
        except:
            exMsg = '*' + traceback.format_exc()
            print('exMsg:' + exMsg)
        finally:
            if 'r' in locals() and r:
                r.close()

        if exMsg or r.status_code != 200:
            return set()

        html = r.content.decode('utf-8')

        soup = BeautifulSoup(html, 'html.parser')
        ipTable = soup.find('table', id='ip_list')
        if not ipTable:
            print('Error: not ipTable')
            return set()

        for trTag in ipTable.find_all('tr'):
            # print('trTag:' + repr(trTag))
            lst = list(trTag.find_all('td'))
            # print('lst size:' + str(len(lst)))
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
        sql = "SELECT DISTINCT proxy FROM proxy_pool;"
        conn = pymysql.connect(host=DBHost,
                               user=DBUser,
                               passwd=DBPwd,
                               db=DBName,
                               port=DBPort)

        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        # print('results:' + str(type(results)))
        # print(repr(set(results)))
        conn.close()

        proxyPool = set()
        for item in results:
            proxyPool.add(item[0])

        # print('proxyPool:' + repr(proxyPool))

        return proxyPool


# 读取配置文件
def ReadConfig():
    global DBHost, DBPort, DBUser, DBPwd, DBName, DstUrl, Feature, InformAddressPool

    cfg = ConfigParser()
    cfgFile = os.path.join(cur_dir_fullpath, 'config\\config.ini')
    if not os.path.exists(cfgFile):
        input(cfgFile + ' not found')
        exit(-1)
    cfgLst = cfg.read(cfgFile)
    if len(cfgLst) < 1:
        input('Read config.ini failed...')
        sys.exit(-1)

    DBHost = cfg.get('ProxyGather', 'DBHost')  # 数据库地址
    DBPort = cfg.get('ProxyGather', 'DBPort')  # 数据库端口
    DBUser = cfg.get('ProxyGather', 'DBUser')
    DBPwd = cfg.get('ProxyGather', 'DBPwd')
    DBName = cfg.get('ProxyGather', 'DBName')
    DstUrl = cfg.get('ProxyGather', 'DstUrl')
    Feature = cfg.get('ProxyGather', 'Feature')

    DBHost = DBHost.strip()
    DBPort = int(DBPort.strip())
    DBUser = DBUser.strip()
    DBPwd = DBPwd.strip()
    DBName = DBName.strip()
    DstUrl = DstUrl.strip()
    Feature = Feature.strip()

    print('DBHost:' + DBHost)
    print('DBPort:' + str(DBPort))
    print('DBUser:' + DBUser)
    print('DBPwd:' + DBPwd)
    print('DBName:' + DBName)
    print('DstUrl:' + DstUrl)
    print('Feature:' + Feature)

    InformAddress = cfg.get('ProxyGather', 'InformAddress')  # 获取需要通知更新的地址
    setTmp = set(InformAddress.split(';'))
    for addr in setTmp:
        if not addr.find(':'):
            continue
        InformAddressPool.add(addr)

    print('Read config.ini successed!\n')


def UpdateProxyPool():
    ProxyPoolTotal = set()

    proxyPool = ProxyGather.readDBProxy()  # 数据库中的代理地址
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(db):' + repr(proxyPool))
    print('*** proxyPool(db) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromXICI(1)  # 采集1页(xici)
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(xici):' + repr(proxyPool) + '\n')
    print('*** proxyPool(xici) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromMimvp(600)  # mimvp api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(mimvp):' + repr(proxyPool))
    print('*** proxyPool(mimvp) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromDaili666(600)  # daili666 api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(daili666):' + repr(proxyPool))
    print('*** proxyPool(daili666) size:' + str(len(proxyPool)) + '\n')

    proxyPool = ProxyGather.getProxyFromKuaidaili(500)  # kuaidaili api
    ProxyPoolTotal = ProxyPoolTotal.union(proxyPool)
    # print('*** proxyPool(kuaidaili):' + repr(proxyPool))
    print('*** proxyPool(kuaidaili) size:' + str(len(proxyPool)) + '\n')

    print('*** ProxyPoolTotal size:' + str(len(ProxyPoolTotal)) + '\n')

    ProxyPoolValid = ProxyPoolTotal  # 先假定所有地址有效

    print('\nCheck for %s, %s ... ' % (DstUrl, Feature))
    ProxyPoolValid = ProxyGather.getValidProxyPool(ProxyPoolValid, DstUrl, Feature)
    print('****** ProxyPoolValid size:' + str(len(ProxyPoolValid)) + ' ******')

    '''
    if len(ProxyPoolValid) < 5:
        print('ProxyPoolValid too small, no write database!!!')
        return
    '''
    print('Write DataBase ...')
    conn = pymysql.connect(host=DBHost,
                           user=DBUser,
                           passwd=DBPwd,
                           db=DBName,
                           port=DBPort)
    cursor = conn.cursor()
    sql = "DELETE  FROM proxy_pool;"
    cursor.execute(sql)  # 清空数据库表
    sql = "insert into proxy_pool(proxy)values(%s)"
    cursor.executemany(sql, list(ProxyPoolValid))  # 写入数据库
    conn.commit()
    conn.close()
    print('Write DataBase over.')

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


if __name__ == "__main__":

    ReadConfig()  # 读取配置文件

    sleepSpan = 20
    while True:
        startTime = time.time()  # 开始时刻
        UpdateProxyPool()
        print('****** update timespan:' + '{:.2f}s'.format(time.time() - startTime) + ' ******')
        print('current time:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        print('Program will sleep %d s \n\n' % sleepSpan)
        time.sleep(sleepSpan)
