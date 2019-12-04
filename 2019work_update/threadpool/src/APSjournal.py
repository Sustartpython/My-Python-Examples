from Provider import Provider
import utils
import requests
import os
import redis
import time
import re
import json
import traceback
import threading
from bs4 import BeautifulSoup
from Const import MQQueueFinish


class APSJournal(Provider):

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
        self.mapfunc['startdown_html'] = self.startdown_html
        self.mapfunc['down_html'] = self.down_html
        self.mapfunc['process_html'] = self.process_html
        self.mapfunc['parse_html'] = self.parse_html
        self.mapfunc['process_update'] = self.process_update
        self.mapfunc['upload2HDFS'] = self.upload2HDFS

    def update(self):
        self.count = 0
        self.initpath()
        self.refreshproxypool()
        gchlist = [
            'prl', 'prx', 'rmp', 'pra', 'prb', 'prc', 'prd', 'pre', 'prab', 'prapplied', 'prfluids', 'prmaterials',
            'prper', 'ppf'
        ]
        self.totalcount = len(gchlist)
        for gch in gchlist:
            url = 'https://journals.aps.org/{}/issues'.format(gch)
            func = 'process_update'
            self.sendwork('down_html', (url, gch, func))

    def process_update(self, message):
        self.count = self.count + 1
        utils.printf(message)
        if self.count == self.totalcount:
            utils.printf('下载起始页完成')
            self.sendwork('parse_index', self.html_path)

    def startdown_html(self, message):
        self.count = 0
        self.initpath()
        self.refreshproxypool()
        gchlist = [
            'prl', 'prx', 'rmp', 'pra', 'prb', 'prc', 'prd', 'pre', 'prab', 'prapplied', 'prfluids', 'prmaterials',
            'prper', 'ppf'
        ]
        self.totalcount = len(gchlist)
        for gch in gchlist:
            url = 'https://journals.aps.org/{}/issues'.format(gch)
            func = 'process_html'
            self.sendwork('down_html', (url, gch, func))

    def down_html(self, message):
        url = message[0]
        gch = message[1]
        func = message[2]
        feature = 'volume-issue-list'
        fname = self.html_path + '/' + gch + '.html'
        if os.path.exists(fname):
            utils.printf(gch)
            self.senddistributefinish(func)
            return
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish(func)

    def process_html(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('下载起始页完成')
            self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'apsjournal')
        result = []
        stmt = 'insert ignore into volume(url,stat) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            divlist = soup.select('div.volume-issue-list')
            for divTag in divlist:
                url = 'https://journals.aps.org' + divTag.h4.a.get('href')
                result.append((url, 0))
            if utils.parse_results_to_sql(conn, stmt, result, 100):
                cnt += len(result)
                result.clear()
                utils.printf(cnt)
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.senddistributefinish('startdown_index')

    def startdown_index(self, message):
        utils.printf('%s:开始下载索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'apsjournal')
        cur = conn.cursor()
        cur.execute('select url,stat from volume where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.index_path)) == 0:
                utils.logerror('%s:没有新的volume不需要更新' % self.provider)
            else:
                self.sendwork('parse_index', self.index_path)
        for url, _ in rows:
            fname = self.index_path + '/' + url.split('/')[-3] + '_' + url.split('/')[-1] + '.html'
            self.sendwork('down_index', (url, fname))

    def down_index(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index', url)
            return
        feature = 'volume-issue-list'
        resp = self.gethtml(url, feature)
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
            conn = utils.init_db('mysql', 'apsjournal')
            stmt = 'update volume set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'apsjournal')
            stmt = 'update volume set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('down_index finish')
            self.sendwork('parse_index', self.index_path)

    def parse_index(self, message):
        workdir = message
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'apsjournal')
            result = []
            stmt = 'insert ignore into issue(url,year) Values(%s,%s) on DUPLICATE key UPDATE year=%s'
            cnt = 0
            for filename, fullname in utils.file_list(workdir):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                liTags = soup.select('div.volume-issue-list > ul > li')
                for liTag in liTags:
                    yeartext = liTag.get_text().strip()
                    year = re.sub('.*?(\d{4}) \(.*?\)', r'\1', yeartext)
                    url = 'https://journals.aps.org' + liTag.b.a.get('href')
                    result.append((url, year, year))
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
        conn = utils.init_db('mysql', 'apsjournal')
        cur = conn.cursor()
        current_year = time.strftime('%Y')
        cur.execute(
            "select url,stat from issue where stat=0 or year=%s or year=%s" % (current_year, int(current_year) - 1)
        )
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                self.sendwork('parse_list')
        for url, _ in rows:
            fdir = self.list_path + '/' + url.split('/')[-4]
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
        feature = 'large-9 columns'
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_list', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list', url)

    def process_list(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message))

        if self.count % 40 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'apsjournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'apsjournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'apsjournal')
        result = []
        stmt = 'insert ignore into article(url,vol,issue) Values(%s,%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            vol = filename.split('_')[0]
            issue = filename.split('_')[-1].replace('.html', '')
            soup = BeautifulSoup(text, 'lxml')
            aTags = soup.select('div.large-9.columns > h5 > a')
            for aTag in aTags:
                url = aTag.get('href')
                if not url.startswith('/'):
                    continue
                url = 'https://journals.aps.org' + url
                result.append((url, vol, issue))
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
        conn = utils.init_db('mysql', 'apsjournal')
        cur = conn.cursor()
        cur.execute('select url,vol,issue from article where stat=0 and failcount<20 limit 10000')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('upload2HDFS')
            return
        for url, vol, issue in rows:
            self.sendwork('down_detail', (url, vol, issue))

    def down_detail(self, message):
        url = message[0]
        vol = message[1]
        issue = message[2]

        feature = 'medium-9 columns'
        resp = self.gethtml(url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (url, False))
            return

        htmlText = resp.content.decode('utf8').strip()

        aurl = url.replace('abstract', 'authors')
        res = self.getauthors(aurl)
        if not res:
            self.senddistributefinish('process_detail', (url, False))
            return

        authorText = res.content.decode('utf8').strip()

        sumDict = dict()
        sumDict['url'] = url
        sumDict['vol'] = vol
        sumDict['issue'] = issue
        sumDict['detail'] = htmlText
        sumDict['authors'] = authorText

        # 每个线程单独写入一个文件，无需加锁
        outPathFile = os.path.join(
            self.detail_path,
            '%s_%d_%d.big_json' % (self.detail_path.split('\\')[-2], os.getpid(), threading.get_ident())
        )
        utils.printf('Write to %s ...' % outPathFile)
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
            conn = utils.init_db('mysql', 'apsjournal')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            self.refreshproxypool()
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

    def getauthors(self, url):
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(url, timeout=20, proxies=proxies)
            if resp.status_code != 200:
                print('code !=200')
                return False
            if resp.content.decode('utf-8').find('</p>') < 0:
                print('not endwith </p>')
                return False
        except:
            return False
        return resp

    def startmission(self):
        ConnRabbitMQ = Provider.OpenConnRabbitMQ()
        channel = ConnRabbitMQ.channel()  # 创建频道
        dic = self.package('startdown_list')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneAPSJournal = APSJournal('apsjournal', 'proxy_aps', '/RawData/apsjournal/big_json')

if __name__ == '__main__':
    oneAPSJournal.startmission()
