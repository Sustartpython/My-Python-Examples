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
import urllib


class BioOneJournal(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.mapfunc['down_index'] = self.down_index
        self.mapfunc['process_index'] = self.process_index
        self.mapfunc['startdown_index'] = self.startdown_index
        self.mapfunc['parse_index'] = self.parse_index
        self.mapfunc['startdown_indexlist'] = self.startdown_indexlist
        self.mapfunc['down_indexlist'] = self.down_indexlist
        self.mapfunc['process_indexlist'] = self.process_indexlist
        self.mapfunc['parse_indexlist'] = self.parse_indexlist
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
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['mapcover'] = self.mapcover

    def update(self):
        self.startdown_html(None)

    def startdown_html(self, message):
        self.initpath()
        self.refreshproxypool()
        alphabetlist = [
            'A', 'B', 'C', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'U', 'W', 'Z'
        ]
        self.totalcount = len(alphabetlist)
        for alpha in alphabetlist:
            self.sendwork('down_html', alpha)

    def down_html(self, message):
        url = 'https://bioone.org/browse/title/%s' % message
        feature = 'journal BrowseTitleAll'
        fname = '%s/%s.html' % (self.html_path, message)
        if os.path.exists(fname):
            self.senddistributefinish('process_html')
            return
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_html')

    def process_html(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:down_html finish' % self.provider)
            self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'bioonejournal')
        result = []
        stmt = 'insert ignore into journal(url,cover_url) Values(%s,%s) on DUPLICATE key UPDATE cover_url=%s'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            aTaglist = soup.select('div.journal.BrowseTitleAll > a')
            for aTag in aTaglist:
                url = aTag.get('href')
                if url == "/journals/":
                    continue
                if url.startswith('/ebooks'):
                    continue
                cover_url = aTag.img.get('src')
                result.append((url, cover_url, cover_url))
            utils.parse_results_to_sql(conn, stmt, result)
            cnt += len(result)
            result.clear()
            utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.senddistributefinish('startdown_indexlist')

    def startdown_indexlist(self, message):
        utils.printf('%s:开始下载期索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        path = '%s/%s' % (self.datepath, 'indexlist')
        if not os.path.exists(path):
            os.makedirs(path)
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'bioonejournal')
        cur = conn.cursor()
        cur.execute('select url,cover_url from journal')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        for url, _ in rows:
            fname = path + '/' + url.split('/')[-1] + '.html'
            self.sendwork('down_indexlist', (url, fname))

    def down_indexlist(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_indexlist', url)
            return
        feature = 'row IssueByYearInnerRow'
        print("----------%s-----------" % url)
        resp = self.gethtml('https://bioone.org%s/issues' % url, feature)
        if not resp:
            self.sendwork('down_indexlist', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_indexlist', url)

    def process_indexlist(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message))

        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            self.refreshproxypool()
        if self.count == self.totalcount:
            utils.printf('%s:down_indexlist finish' % self.provider)
            self.sendwork('parse_indexlist')

    def parse_indexlist(self, message):
        try:
            utils.printf('%s:解析期索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'bioonejournal')
            self.sqlList.clear()
            cnt = 0
            cur = conn.cursor()
            path = '%s/%s' % (self.datepath, 'indexlist')
            for filename, fullname in utils.file_list(path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                aTags = soup.find_all('a', class_='IssueByYearInnerText')
                for aTag in aTags:
                    url = aTag.get('href').replace('https://bioone.org', '')
                    self.sqlList.append(
                        "insert ignore into issuelist(url,year) Values('%s','%s')" % (url, url.split('/')[-1])
                    )
                cnt += len(self.sqlList)
                for sql in self.sqlList:
                    cur.execute(sql)
                conn.commit()
                self.sqlList.clear()
                utils.printf(cnt)
            cur.close()
            conn.close()
            utils.printf('%s:解析索引页完成...' % self.provider)
            # self.sendwork('down_cover')
            self.senddistributefinish('startdown_index')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def startdown_index(self, message):
        utils.printf('%s:开始下载索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'bioonejournal')
        cur = conn.cursor()
        cur.execute('select url,year from issuelist where year>=2018 or stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.index_path)) == 0:
                utils.logerror('%s:没有新的issuelist不需要更新' % self.provider)
            else:
                # self.sendwork('down_cover')
                self.sendwork('parse_index')
        for url, year in rows:
            fname = '%s/%s_%s.html' % (self.index_path, url.split('/')[-3], year)
            self.sendwork('down_index', (url, fname))

    def down_index(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index', url)
            return
        feature = 'row IssueByYearInnerRow'
        resp = self.gethtml('https://bioone.org' + url, feature)
        if not resp:
            self.sendwork('down_index', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_index', url)

    def process_index(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message))
        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'bioonejournal')
            stmt = 'update issuelist set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'bioonejournal')
            stmt = 'update issuelist set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('%s:down_index finish' % self.provider)
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'bioonejournal')
            self.sqlList.clear()
            cnt = 0
            cur = conn.cursor()
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                aTags = soup.select('div.row.JournasBrowseRowPadding1 > div > div > a')
                if len(aTags) == 0:
                    aTags = soup.select('div.row.JournasBrowseRowPadding1 > div > a')
                for aTag in aTags:
                    url = aTag.get('href').replace('https://bioone.org', '')
                    self.sqlList.append("insert ignore into issue(url,stat) Values('%s',%d)" % (url, 0))
                cnt += len(self.sqlList)
                for sql in self.sqlList:
                    cur.execute(sql)
                conn.commit()
                self.sqlList.clear()
                utils.printf(cnt)
            cur.close()
            conn.close()
            utils.printf('%s:解析索引页完成...' % self.provider)
            # self.sendwork('down_cover')
            self.senddistributefinish('startdown_list')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def startdown_list(self, message):
        utils.printf('%s:开始下载列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'bioonejournal')
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
        for url, _ in rows:
            fdir = self.list_path + '/' + url.split('/')[2]
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            fname = fdir + '/' + url.split('/')[-2] + '_' + url.split('/')[-1] + '.html'
            self.sendwork('down_list', (url, fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list', url)
            return
        feature = 'row TOCLineItemRow1'
        resp = self.gethtml('https://bioone.org' + url, feature)
        if not resp:
            utils.printf(url)
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
            conn = utils.init_db('mysql', 'bioonejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'bioonejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'bioonejournal')
        result = []
        stmt = 'insert ignore into article(url,vol,issue,gch) Values(%s,%s,%s,%s)'
        # stmt = 'update article set gch=%s where url=%s'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            vol = filename.split('_')[0].replace('volume-', '')
            if not vol.isdigit():
                utils.logerror(fullname + ' vol不是数字\n')
                vol = ''
            issue = filename.split('_')[-1].replace('.html', '').replace('issue-', '').replace(r'%e2%80%93', '-')
            issue = urllib.parse.unquote(issue)
            gch = fullname.split('\\')[-2]
            soup = BeautifulSoup(text, 'lxml')
            aTags = soup.select('a.TocLineItemAnchorText1')
            if len(aTags) == 0:
                utils.logerror(fullname + '\n')
            for aTag in aTags:
                url = aTag.get('href')
                # if not url.startswith('/'):
                #     continue
                url = urllib.parse.unquote(url)

                result.append((url, vol, issue, gch))
                # result.append((gch, url))
            if utils.parse_results_to_sql(conn, stmt, result, 1000):
                cnt += len(result)
                result.clear()
                utils.printf(cnt)
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析列表页完成...' % self.provider)
        self.senddistributefinish('startdown_detail')

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'bioonejournal')
        cur = conn.cursor()
        cur.execute('select url,vol,issue,gch from article where stat=0 and failcount<20 limit 10000')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('upload2HDFS')
            self.sendwork('down_cover')
            return
        for url, vol, issue, gch in rows:
            self.sendwork('down_detail', (url, vol, issue, gch))

    def down_detail(self, message):
        url = message[0]
        vol = message[1]
        issue = message[2]
        gch = message[3]

        feature = 'citation_title'
        resp = self.gethtml('https://bioone.org' + url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (url, False))
            return

        htmlText = resp.content.decode('utf8').strip()

        sumDict = dict()
        sumDict['url'] = url
        sumDict['vol'] = vol
        sumDict['issue'] = issue
        sumDict['gch'] = gch
        sumDict['down_date'] = time.strftime('%Y%m%d', time.localtime())
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
        self.senddistributefinish('process_detail', (url, True))

    def process_detail(self, message):
        self.count = self.count + 1
        url = message[0]
        flag = message[1]
        if flag:
            sql = "update article set stat=1 where url='{}'".format(url)
        else:
            sql = "update article set failcount=failcount+1 where url='{}'".format(url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 200 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'bioonejournal')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
        if self.totalcount == self.count:
            self.startdown_detail(None)

    def gethtml(self, url, feature=None):
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(url, headers=HEADER, timeout=20, proxies=proxies)
            if resp.status_code != 200:
                print('code !=200')
                return False
            if feature:
                if resp.content.decode('utf-8').find(feature) < 0:
                    print('can not find feature')
                    return False
            if resp.content.decode('utf-8').find('</html>') < 0:
                print('not endwith </html>')
                return False
        except:
            # exMsg = '* ' + traceback.format_exc()
            # print(exMsg)
            # utils.logerror(exMsg)
            return False
        return resp

    def down_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'bioonejournal')
        cur = conn.cursor()
        cur.execute('select url,cover_url from journal')
        rows = cur.fetchall()
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        proxy = self.getproxy()
        for url, cover_url in rows:
            filename = self.cover_path + '/' + url.split('/')[-1] + '.jpg'
            if os.path.exists(filename):
                continue

            cover_url = 'https://bioone.org' + cover_url
            while True:
                try:
                    proxies = {'http': proxy, 'https': proxy}
                    resp = requests.get(cover_url, headers=HEADER, timeout=20, proxies=proxies)
                    # resp = requests.get(cover_url, headers=HEADER, timeout=20)
                except:
                    utils.printf(filename)
                    proxy = self.getproxy()
                    continue
                if utils.Img2Jpg(resp.content, filename):
                    utils.printf('下载图片%s成功' % filename)
                    break
        self.sendwork('mapcover')

    def mapcover(self, message):
        nCount = 0
        provider = 'bioonejournal'
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
        dic = self.package('startdown_list')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneBioOneJournal = BioOneJournal('bioonejournal', 'proxy_cnki', '/RawData/bioonejournal/big_json')

if __name__ == '__main__':
    oneBioOneJournal.startmission()