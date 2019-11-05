from Provider import Provider
import utils
import requests
import os
import redis
import time
import json
import traceback
import threading
import re
import operator
from bs4 import BeautifulSoup
from Const import MQQueueFinish


def changetonum(string):
    plist = re.findall('\((.*?)\)', string)
    pnum = ''
    for p in plist:
        m = p.replace('!![]', '1').replace('!+[]', '1').replace('[]', '0')
        to = 0
        for num in m.split('+'):
            if num.isdigit():
                to = to + int(num)
        pnum = pnum + str(to)
    return int(pnum)


def getresult(string):
    pt = re.compile('\+\((\(.*?\))\)(.)\+\((\(.*?\))\)')
    gb = pt.match(string)
    num1 = changetonum(gb.group(1))
    num2 = changetonum(gb.group(3))
    operate = gb.group(2)
    if operate == '+':
        result = num1 + num2
    elif operate == '-':
        result = num1 - num2
    elif operate == '*':
        result = num1 * num2
    elif operate == '/':
        result = num1 / num2
    print('%s%s%s=%s' % (num1, operate, num2, result))
    return result


class CambridgeJournal(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.session = None
        self.proxies = None
        self.refreshflag = False
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        self.mapfunc['down_index'] = self.down_index
        self.mapfunc['process_index'] = self.process_index
        self.mapfunc['startdown_index'] = self.startdown_index
        self.mapfunc['parse_index'] = self.parse_index
        self.mapfunc['down_list'] = self.down_list
        self.mapfunc['process_list'] = self.process_list
        self.mapfunc['startdown_list'] = self.startdown_list
        self.mapfunc['parse_list'] = self.parse_list
        self.mapfunc['down_detail'] = self.down_detail
        self.mapfunc['process_detail'] = self.process_detail
        self.mapfunc['startdown_detail'] = self.startdown_detail
        self.mapfunc['startdown_html'] = self.startdown_html
        self.mapfunc['down_html'] = self.down_html
        self.mapfunc['process_html'] = self.process_html
        self.mapfunc['parse_html'] = self.parse_html
        self.mapfunc['startdown_cover'] = self.startdown_cover
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['process_cover'] = self.process_cover
        self.mapfunc['mapcover'] = self.mapcover
        self.mapfunc['upload2HDFS'] = self.upload2HDFS

    def refreshsession(self):
        self.refreshflag = True
        url = 'https://www.cambridge.org'
        utils.printf('%s:开始刷新session' % self.provider)
        while True:
            try:
                sn = requests.session()
                proxy = self.getproxy()
                proxies = {'http': proxy, 'https': proxy}
                resp = sn.get(url, headers=self.headers, timeout=20, proxies=proxies)
                if resp.content.decode('utf-8').find('Just a moment...') > 0:
                    utils.printf('Just a moment...')
                else:
                    utils.printf('特殊情况...')
                    if resp.content.decode('utf-8').find('Latest News') > 0:
                        self.session = sn
                        self.proxies = proxies
                        self.refreshflag = False
                        return True
                    else:
                        utils.logerror(resp.text)
                        continue
                soup = BeautifulSoup(resp.text, 'lxml')
                inputlist = soup.select('#challenge-form > input')
                for tag in inputlist:
                    if operator.eq(tag.get('name'), 'jschl_vc'):
                        jschl_vc = tag.get('value')
                        print(jschl_vc)
                    elif operator.eq(tag.get('name'), 'pass'):
                        password = tag.get('value')
                        print(password)
                ma = re.findall(':(\+\(\(.*?\)\).\+\(\(.*?\)\))', resp.text)
                mb = re.findall('(.)=(\+\(\(.*?\)\).\+\(\(.*?\)\))', resp.text)
                result = getresult(ma[0])

                for operate, string in mb:
                    if operate == '+':
                        result = result + getresult(string)
                    elif operate == '-':
                        result = result - getresult(string)
                    elif operate == '*':
                        result = result * getresult(string)
                    elif operate == '/':
                        result = result / getresult(string)

                result = round(result + 17, 10)
                uri = 'https://www.cambridge.org/cdn-cgi/l/chk_jschl?jschl_vc=%s&pass=%s&jschl_answer=%s' % (
                    jschl_vc, password, result
                )
                utils.printf('等待ddos检测')
                time.sleep(5)
                resp = sn.get(uri, headers=self.headers, timeout=20, proxies=proxies)
                if resp.status_code == 200:
                    utils.printf('%s:刷新session成功' % self.provider)
                    self.session = sn
                    self.proxies = proxies
                    self.refreshflag = False
                    return True
            except:
                exMsg = '* ' + traceback.format_exc()
                print(exMsg)
                continue

    def update(self):
        self.startdown_html(None)

    def startdown_html(self, message):
        self.initpath()
        self.refreshproxypool()
        url = 'https://www.cambridge.org/core/what-we-publish/journals'
        self.sendwork('down_html', url)

    def down_html(self, message):
        url = message
        feature = 'product-list-entry'
        fname = self.html_path + '/html.html'
        if os.path.exists(fname):
            self.senddistributefinish('process_html')
            return
        self.refreshsession()
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_html')

    def process_html(self, message):
        utils.printf('下载起始页完成')
        self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'cambridgejournal')
        result = []
        stmt = 'insert ignore into journal(url,cover_url) Values(%s,%s)'
        cnt = 0
        fname = self.html_path + '/html.html'
        with open(fname, encoding='utf8') as f:
            text = f.read()
        soup = BeautifulSoup(text, 'lxml')
        aTaglist = soup.select('ul.listings > li > div > div > a')
        for aTag in aTaglist:
            url = aTag.get('href')
            cover_url = ''
            result.append((url, cover_url))
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.sendwork('startdown_index')

    def startdown_index(self, message):
        utils.printf('%s:开始下载索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.refreshsession()
        self.count = 0
        conn = utils.init_db('mysql', 'cambridgejournal')
        cur = conn.cursor()
        cur.execute('select url,cover_url from journal')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        for url, _ in rows:
            fname = self.index_path + '/' + url.split('/')[-1] + '.html'
            self.sendwork('down_index', (url, fname))

    def down_index(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index', url)
            return
        feature = 'id="maincontent"'
        resp = self.gethtml('https://www.cambridge.org' + url + '/all-issues', feature)
        if not resp:
            self.sendwork('down_index', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_index', url)

    def process_index(self, message):
        self.count = self.count + 1
        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            self.refreshproxypool()
        if self.count == self.totalcount:
            utils.printf('down_index finish')
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'cambridgejournal')
            self.sqlList.clear()
            cnt = 0
            cur = conn.cursor()
            stmt = "insert ignore into issue(url,stat) Values(%s,%s)"
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                aTags = soup.select('div > ul > li > ul > li > a.row')
                if len(aTags) == 0:
                    aTags = soup.select('div > ul > li > a.row.item')
                    if len(aTags) == 0:
                        utils.logerror(filename)
                for aTag in aTags:
                    url = aTag.get('href').replace('https://www.cambridge.org', '')
                    self.sqlList.append((url, 0))
                if utils.parse_results_to_sql(conn, stmt, self.sqlList, 1000):
                    cnt += len(self.sqlList)
                    self.sqlList.clear()
                    utils.printf(cnt)
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            cnt += len(self.sqlList)
            self.sqlList.clear()
            utils.printf(cnt)
            cur.close()
            conn.close()
            utils.printf('%s:解析索引页完成...' % self.provider)
            self.senddistributefinish('startdown_list')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def startdown_list(self, message):
        utils.printf('%s:开始下载列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'cambridgejournal')
        cur = conn.cursor()
        cur.execute('select url,stat from issue where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                # self.sendwork('down_cover')
                self.sendwork('parse_list')
                return
        self.refreshsession()
        for url, _ in rows:
            fdir = self.list_path + '/' + url.split('/')[-3]
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            flast = url.split('/')[-1]
            if flast.find('?pageNum=') > 0:
                flast = flast.split('?')[0] + '_' + flast.split('=')[-1]
            fname = fdir + '/' + flast + '.html'
            self.sendwork('down_list', (url, fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list', url)
            return
        feature = 'details'
        resp = self.gethtml('https://www.cambridge.org' + url, feature)
        if not resp:
            print(url)
            self.sendwork('down_list', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list', url)

    def process_list(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message))

        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'cambridgejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'cambridgejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'cambridgejournal')
        result = []
        issueresult = []
        stmt = 'insert ignore into article(uid,url,gch) Values(%s,%s,%s)'
        sql = 'insert ignore into issue(url,stat) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            gch = fullname.split('\\')[-2]
            soup = BeautifulSoup(text, 'lxml')
            ulTags = soup.select('ul.details')
            if len(ulTags) == 0:
                utils.logerror(fullname + '\n')
            for ulTag in ulTags:
                try:
                    aTag = ulTag.select_one('li.title > a')
                    if not aTag:
                        aTag = ulTag.select_one('li.title > h5 > a')
                    if aTag:
                        url = aTag.get('href')
                        uid = url.split('/')[-1]
                        result.append((uid, url, gch))
                except:
                    utils.printf(fullname)
                    utils.logerror(fullname)
                    break
            if filename.find('_') < 0:
                pageTag = soup.select_one('ul.pagination')
                if pageTag:
                    pTags = pageTag.select('li > a')
                    for pTag in pTags:
                        if operator.eq(pTag.get_text(), 'Last'):
                            pagenum = int(pTag.get('data-page-number'))
                            for page in range(2, pagenum + 1):
                                uri = '/core/journals/%s/issue/%s?pageNum=%s' % (gch, filename.replace('.html', ''), page)
                                issueresult.append((uri, 0))
            if utils.parse_results_to_sql(conn, stmt, result, 1000):
                cnt += len(result)
                result.clear()
                utils.printf(cnt)
        utils.parse_results_to_sql(conn, stmt, result)
        utils.parse_results_to_sql(conn, sql, issueresult)
        cnt += len(result)
        utils.printf(cnt)
        utils.printf('大于一页的个数为%s' % len(issueresult))
        conn.close()
        utils.printf('%s:解析列表页完成...' % self.provider)
        self.senddistributefinish('startdown_detail')

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        if not self.session:
            self.refreshsession()
        self.count = 0
        conn = utils.init_db('mysql', 'cambridgejournal')
        cur = conn.cursor()
        cur.execute('select uid,url,gch from article where stat=0 and failcount<20 limit 10000')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('upload2HDFS')
            self.sendwork('startdown_cover')
            return
        for uid, url, gch in rows:
            self.sendwork('down_detail', (uid, url, gch))

    def down_detail(self, message):
        uid = message[0]
        url = message[1]
        gch = message[2]

        feature = 'class="article-title"'
        resp = self.gethtml('https://www.cambridge.org' + url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (uid, False))
            return

        htmlText = resp.content.decode('utf8').strip()

        sumDict = dict()
        sumDict['uid'] = uid
        sumDict['url'] = url
        sumDict['gch'] = gch
        sumDict['detail'] = htmlText

        # 每个线程单独写入一个文件，无需加锁
        outPathFile = os.path.join(
            self.detail_path,
            '%s_%d_%d.big_json' % (self.detail_path.split('\\')[-2], os.getpid(), threading.get_ident())
        )
        print('Write to %s ...' % outPathFile)
        with open(outPathFile, mode='a', encoding='utf-8') as f:
            line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
            f.write(line)
        self.senddistributefinish('process_detail', (uid, True))

    def process_detail(self, message):
        self.count = self.count + 1
        uid = message[0]
        flag = message[1]
        if flag:
            sql = "update article set stat=1 where uid='{}'".format(uid)
        else:
            sql = "update article set failcount=failcount+1 where uid='{}'".format(uid)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 200 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'cambridgejournal')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
        if self.totalcount == self.count:
            self.startdown_detail(None)

    def upload2HDFS(self, message):
        utils.all_2_one(self.detail_path, self.merge_path)
        flag = utils.ProcAll(self.merge_path, self.hdfs_path)
        if flag:
            utils.msg2weixin('%s:bigjson成功上传至%s' % (self.provider, self.hdfs_path))
        else:
            utils.logerror('%s:bigjson上传至%s出现问题' % (self.provider, self.hdfs_path))
            utils.msg2weixin('%s:bigjson上传至%s出现问题' % (self.provider, self.hdfs_path))

    def gethtml(self, url, feature=None, coverflag=False):
        try:
            resp = self.session.get(url, headers=self.headers, timeout=20, proxies=self.proxies)
            if not coverflag:
                if resp.content.decode('utf-8').find('Just a moment...') > 0:
                    utils.printf('Just a moment...')
                    if not self.refreshflag:
                        self.refreshflag = True
                        self.refreshsession()
                    else:
                        while self.refreshflag:
                            time.sleep(1)
                    resp = self.session.get(url, headers=self.headers, timeout=20, proxies=self.proxies)
                if resp.status_code != 200:
                    print('code !=200')
                    return False
                if resp.content.decode('utf-8').find('</html>') < 0:
                    print('not endwith </html>')
                    return False
                if feature:
                    if resp.content.decode('utf-8').find(feature) < 0:
                        print('can not find feature')
                        utils.logerror(url)
                        return False
        except:
            return False
        return resp

    def startdown_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'cambridgejournal')
        cur = conn.cursor()
        cur.execute('select url,cover_url from journal')
        rows = cur.fetchall()
        self.refreshsession()
        self.count = 0
        self.totalcount = len(rows)
        for url, cover_url in rows:
            filename = self.cover_path + '/' + url.split('/')[-1] + '.jpg'
            self.sendwork('down_cover', (url, filename))

    def down_cover(self, message):
        url = message[0]
        filename = message[1]
        if os.path.exists(filename):
            self.senddistributefinish('process_cover')
            return
        feature = 'on-the-cover-homepage'
        utils.logerror(url)
        utils.printf(url)
        resp = self.gethtml('https://www.cambridge.org' + url)
        if not resp:
            self.sendwork('down_cover', message)
            return
        if resp.content.decode('utf-8').find(feature) < 0:
            print("can not find on-the-cover-homepage")
            feature = 'row cover collapse'
            resp = self.gethtml('https://www.cambridge.org' + url + '/latest-issue', feature)
            if not resp:
                self.sendwork('down_cover', message)
                return
        soup = BeautifulSoup(resp.content.decode('utf-8'), 'lxml')
        imgtag = soup.select_one('div.image.on-the-cover-homepage > a > img')
        if not imgtag:
            imgtag = soup.select_one('div.large-4.medium-4.small-12.columns > div > div > div > a > img')
        if not imgtag:
            utils.logerror('%s：图片错误' % url)
            self.senddistributefinish('process_cover')
            return
        cover_url = 'https:' + imgtag.get('src').replace('https:', '')
        resp = self.gethtml(cover_url, feature=None, coverflag=True)
        if not resp:
            self.sendwork('down_cover', message)
            return
        if utils.Img2Jpg(resp.content, filename):
            utils.printf('下载图片%s成功' % filename)
            self.senddistributefinish('process_cover')
        else:
            utils.printf('%s：图片错误' % url)
            utils.logerror('%s：图片错误' % url)
            self.senddistributefinish('process_cover')

    def process_cover(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:下载图片完成' % self.provider)
            self.sendwork('mapcover')

    def mapcover(self, message):
        if not self.datepath:
            self.initpath()
        nCount = 0
        provider = 'cambridgejournal'
        filePath = self.datepath + '/' + provider + '_cover.txt'
        with open(filePath, mode='w', encoding='utf-8') as f:
            for path, dirNames, fileNames in os.walk(self.cover_path):
                for fileName in fileNames:
                    journal = os.path.splitext(fileName)[0]
                    line = provider + '@' + journal + '★/smartlib/' + provider + '/' + fileName + '\n'
                    f.write(line)
                    nCount += 1
        utils.printf('nCount:' + str(nCount))

    def startmission(self):
        ConnRabbitMQ = Provider.OpenConnRabbitMQ()
        channel = ConnRabbitMQ.channel()  # 创建频道
        dic = self.package('mapcover')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneCambridgeJournal = CambridgeJournal('cambridgejournal', 'proxy_aip', '/RawData/cambridgejournal/big_json')

if __name__ == '__main__':
    oneCambridgeJournal.startmission()