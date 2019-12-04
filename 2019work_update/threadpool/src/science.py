from Provider import Provider
import utils
import requests
import os
import redis
import time
import json
import traceback
import threading
from bs4 import BeautifulSoup
from Const import MQQueueFinish


class ScienceJournal(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
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
        self.mapfunc['startdown_cover'] = self.startdown_cover
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['mapcover'] = self.mapcover
        self.mapfunc['upload2HDFS'] = self.upload2HDFS

    def update(self):
        self.initpath()
        self.refreshproxypool()
        self.count = 0
        self.totalcount = 6
        journallist = ['science', 'advances', 'stm', 'stke', 'immunology', 'robotics']
        curYear = time.localtime().tm_year
        for journalname in journallist:
            self.sendwork('down_index', (journalname, curYear))

    def startdown_index(self, message):
        self.initpath()
        self.refreshproxypool()
        self.count = 0
        total = 0
        mapbeginyear = {
            'science': 1880,
            'advances': 2015,
            'stm': 2009,
            'stke': 1999,
            'immunology': 2016,
            'robotics': 2016
        }
        curYear = time.localtime().tm_year
        for journalname, beginyear in mapbeginyear.items():
            for year in range(beginyear, curYear + 1):
                self.sendwork('down_index', (journalname, year))
                total = total + 1
        self.totalcount = total

    def down_index(self, message):
        try:
            journalname = message[0]
            year = message[1]
            fname = self.index_path + '/' + journalname + '_' + str(year) + '.html'
            utils.printf('开始下载 %s' % fname)
            if os.path.exists(fname):
                self.senddistributefinish('process_index')
                return
            feature = 'issue-month-detail'
            url = 'http://{}.sciencemag.org/content/by/year/{}'.format(journalname, str(year))
            resp = self.gethtml(url, feature)
            if not resp:
                self.sendwork('down_index', (journalname, year))
                return

            with open(fname, mode='w', encoding='utf8') as f:
                f.write(resp.content.decode('utf8'))
            utils.printf('下载 %s 成功' % fname)
            self.senddistributefinish('process_index')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def process_index(self, message):
        self.count = self.count + 1
        if self.count % 20 == 1:
            utils.printf('下载成功 %s 页' % self.count)
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.totalcount == 0:
            return
        elif self.count == self.totalcount:
            utils.printf('%s:downloadindex finish' % self.provider)
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'science')
            result = []
            stmt = 'insert ignore into issue(url,stat) Values(%s,%s)'
            cnt = 0
            for filename, fullname in utils.file_list(self.index_path):
                urlf = '{}.sciencemag.org'.format(filename.split('_')[0])
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                divTags = soup.find_all(
                    'div',
                    class_='highwire-cite highwire-cite-highwire-issue highwire-citation-jnl-sci-issue-archive clearfix'
                )
                for divTag in divTags:
                    url = urlf + divTag.a.get('href')
                    result.append((url, 0))
                if utils.parse_results_to_sql(conn, stmt, result, 1000):
                    cnt += len(result)
                    result.clear()
                    utils.printf(cnt)
            utils.parse_results_to_sql(conn, stmt, result)
            cnt += len(result)
            utils.printf(cnt)
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
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'science')
        cur = conn.cursor()
        cur.execute('select url,stat from issue where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.index_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
                utils.msg2weixin('%s:没有新的issue不需要更新' % self.provider)
            else:
                self.sendwork('parse_list')
        for url, _ in rows:
            fdir = self.list_path + '/' + url.split('.')[0]
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            fname = fdir + '/' + url.split('/')[-2] + '_' + url.split('/')[-1] + '.html'
            url = 'http://' + url
            self.sendwork('down_list', (url, fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list', url)
            return
        feature = 'page-title'
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_list', message)
            return

        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        self.senddistributefinish('process_list', url)

    def process_list(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message.replace('http://', '')))

        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'science')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.totalcount == 0:
            return
        elif self.count == self.totalcount:
            conn = utils.init_db('mysql', 'science')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'science')
        result = []
        stmt = 'insert ignore into article(url,date) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            urlf = '{}.sciencemag.org'.format(fullname.split('\\')[-2])
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            articleTas = soup.find_all(
                'article',
                class_=
                'highwire-cite highwire-cite-highwire-article highwire-citation-jnl-sci-toc clearfix media media--citation'
            )

            for artTag in articleTas:
                try:
                    url = urlf + artTag.div.h3.a.get('href')
                    date = artTag.select_one('time').get_text()
                    result.append((url, date))
                except:
                    exMsg = '* ' + traceback.format_exc()
                    print(fullname + exMsg)
                    utils.logerror(fullname + exMsg)
                    continue
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
        conn = utils.init_db('mysql', 'science')
        cur = conn.cursor()
        cur.execute('select url,date from article where stat=0 and failcount<10 limit 10000')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('down_cover')
            self.sendwork('upload2HDFS')
            return
        for url, date in rows:
            self.sendwork('down_detail', (url, date))

    def down_detail(self, message):
        url = message[0]
        date = message[1]
        vol = url.split('/')[-3]
        issue = url.split('/')[-2]
        feature = 'article__headline'
        resp = self.gethtml('http://' + url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (url, False))
            return

        htmlText = resp.content.decode('utf8').strip()

        sumDict = dict()
        sumDict['url'] = url
        sumDict['date'] = date
        sumDict['vol'] = vol
        sumDict['issue'] = issue
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
            conn = utils.init_db('mysql', 'science')
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

    def gethtml(self, url, feature=None):
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(url, timeout=20, proxies=proxies)
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
            return False
        return resp

    def down_cover(self, message):
        url = 'http://www.sciencemag.org/journals'
        while True:
            resp = self.gethtml(url)
            if resp:
                break
        html = resp.content.decode('utf-8')
        soup = BeautifulSoup(html, 'lxml')
        alist = soup.select('div.media__icon > a')
        for aTag in alist:
            imageTag = aTag.select_one('img')
            filename = self.cover_path + '/' + aTag.get('href').replace('http://', '').split('.')[0] + '.jpg'
            if imageTag:
                imageurl = 'http:' + imageTag.get('src')
                while True:
                    try:
                        resp = requests.get(imageurl)
                    except:
                        continue
                    if utils.Img2Jpg(resp.content, filename):
                        break
        utils.printf('下载图片成功')
        self.sendwork('mapcover')

    def mapcover(self, message):
        nCount = 0
        provider = 'sciencejournal'
        filePath = self.datepath + '/' + provider + '_cover.txt'
        with open(filePath, mode='w', encoding='utf-8') as f:
            for path, dirNames, fileNames in os.walk(self.cover_path):
                for fileName in fileNames:
                    journal = os.path.splitext(fileName)[0]
                    line = provider + '@' + journal + '★/smartlib/' + provider + '/' + fileName + '\n'
                    f.write(line)
                    nCount += 1
        utils.printf('nCount:' + str(nCount))

    def startdown_cover(self, message):
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        self.sendwork('down_cover')

    def startmission(self):
        ConnRabbitMQ = Provider.OpenConnRabbitMQ()
        channel = ConnRabbitMQ.channel()  # 创建频道
        dic = self.package('startdown_cover')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneScienceJournal = ScienceJournal('sciencejournal', 'proxy_sciencemag', '/RawData/sciencejournal/big_json')

if __name__ == '__main__':
    oneScienceJournal.startmission()