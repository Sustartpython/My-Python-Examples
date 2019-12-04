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
from bs4 import BeautifulSoup
from Const import MQQueueFinish


class AsceBook(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.mapfunc['down_list'] = self.down_list
        self.mapfunc['process_list'] = self.process_list
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
        self.mapfunc['parse_detail'] = self.parse_detail

    def update(self):
        self.startdown_html(None)

    def startdown_html(self, message):
        self.initpath()
        self.refreshproxypool()
        self.sendwork('down_html')

    def down_html(self, message):
        typelist = ['book', 'proceedings', 'standards']
        feature = 'listBody'
        for typename in typelist:
            fdir = '%s/%s' % (self.list_path, typename)
            fname = '%s/%s/0.html' % (self.list_path, typename)
            if os.path.exists(fname):
                continue
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            url = 'https://ascelibrary.org/action/showPublications?pubType=%s&sortBy=Ppub&target=browse&pageSize=50&startPage=0' % typename
            while True:
                resp = self.gethtml(url, feature)
                if resp:
                    break
            with open(fname, mode='w', encoding='utf8') as f:
                f.write(resp.content.decode('utf8'))
            utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_html')

    def process_html(self, message):
        utils.printf('下载起始页完成')
        self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        typedic = {'book': 0, 'proceedings': 0, 'standards': 0}
        self.count = 0
        self.totalcount = 0
        for typename in typedic.keys():
            fname = '%s/%s/0.html' % (self.list_path, typename)
            with open(fname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            spanTag = soup.select_one('div.listHeader.clearfix > h3 > span')
            totalpage = (int(spanTag.get_text()) - 1) // 50 + 1
            typedic[typename] = totalpage
            self.totalcount = self.totalcount + totalpage - 1
        for typename in typedic.keys():
            for page in range(1, typedic[typename]):
                self.sendwork('down_list', (typename, page))

    def down_list(self, message):
        typename = message[0]
        page = message[1]
        feature = 'listBody'
        fname = '%s/%s/%d.html' % (self.list_path, typename, page)
        if os.path.exists(fname):
            self.senddistributefinish('process_list')
            return
        url = 'https://ascelibrary.org/action/showPublications?pubType=%s&sortBy=Ppub&target=browse&pageSize=50&startPage=%d' % (
            typename, page
        )
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_list', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list')

    def process_list(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'ascebook')
        result = []
        stmt = 'insert ignore into book(url,cover_url) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            divlist = soup.select('#frmSearchResults > div > div.listBody > div > div.leftSide')
            for divTag in divlist:
                url = divTag.a.get('href')
                isbn = url.split('/')[-1]
                cover_url = ''
                if not isbn.startswith('978'):
                    continue
                coverTag = divTag.a.select_one('img')
                if coverTag:
                    cover_url = coverTag.get('src')
                result.append((url, cover_url))
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
        conn = utils.init_db('mysql', 'ascebook')
        cur = conn.cursor()
        cur.execute('select url,cover_url from book where stat=0 and failcount<20')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.detail_path)) == 0:
                utils.printf('%s:不需要下载详情页' % self.provider)
                return
            utils.printf('%s:下载详情页完成' % self.provider)
            self.senddistributefinish('startdown_cover')
            return
        for url, cover_url in rows:
            self.sendwork('down_detail', (url, cover_url))

    def down_detail(self, message):
        url = message[0]

        fname = '%s/%s.html' % (self.detail_path, url.split('/')[-1])

        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (url, True))
            return

        feature = 'bookInfo__title'
        resp = self.gethtml('https://ascelibrary.org' + url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (url, False))
            return

        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_detail', (url, True))

    def process_detail(self, message):
        self.count = self.count + 1
        url = message[0]
        flag = message[1]
        if flag:
            sql = "update book set stat=1 where url='{}'".format(url)
        else:
            sql = "update book set failcount=failcount+1 where url='{}'".format(url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 50 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'ascebook')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            self.refreshproxypool()
        if self.totalcount == self.count:
            self.startdown_detail(None)

    def parse_detail(self, message):
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, title_series, cover, page, publisher, subject, date, creator_bio, date_created,
        identifier_pisbn, identifier_eisbn, description, identifier_doi, language, country, provider, provider_url,
        provider_id, type, medium, batch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname)
            self.sqlList.append(onemessage)
            if utils.parse_results_to_sql(self.conn, stmt, self.sqlList, 50):
                count += len(self.sqlList)
                utils.printf('%s: 插入 %d 条数据到db3' % (self.provider, count))
                self.sqlList.clear()
        utils.parse_results_to_sql(self.conn, stmt, self.sqlList)
        count += len(self.sqlList)
        utils.printf('%s: 插入 %d 条数据到db3' % (self.provider, count))
        self.conn.close()
        self.conn = None
        utils.msg2weixin('%s: 解析完成,成品文件为%s' % (self.provider, self.template_file))

    def parse_detail_one(self, filename, fullname):
        try:
            language = 'EN'
            country = 'US'
            provider = 'ascebook'
            type_ = 1
            medium = 2
            batch = time.strftime('%Y%m%d') + '00'
            identifier_doi = '10.1061/' + filename.replace('.html', '')
            rawid = identifier_doi
            url = 'https://ascelibrary.org/doi/book/' + identifier_doi
            provider_url = provider + '@' + url
            provider_id = provider + '@' + rawid
            lngid = 'ASCE_TS_' + rawid
            coverpath = '%s/%s.jpg' % (self.cover_path, filename.replace('.html', ''))
            cover = ''
            if os.path.exists(coverpath):
                cover = '/smartlib/ascebook/' + filename.replace('.html', '.jpg')
            publisher = 'American Society of Civil Engineers'
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            title = ''
            titleTag = soup.select_one('div.bookInfo__right > h1')
            if titleTag:
                title = ''.join(titleTag.stripped_strings)

            title_series = ''
            seriesTag = soup.select_one('span.bookInfo__bookset > a')
            if seriesTag:
                title_series = ''.join(seriesTag.stripped_strings)

            description = ''
            descriptionTag = soup.select_one('div.tocList__book__abstract')
            if descriptionTag:
                h2Tag = descriptionTag.select_one('h2')
                if h2Tag:
                    h2Tag.extract()
                description = ''.join(descriptionTag.stripped_strings)

            identifier_pisbn = ''
            pisbnTag = soup.select_one('span.bookInfo__isbn__print')
            if pisbnTag:
                identifier_pisbn = ''.join(pisbnTag.stripped_strings)
                if identifier_pisbn.startswith('ISBN (print):'):
                    identifier_pisbn = identifier_pisbn.split(':')[-1].strip().replace('-', '')

            identifier_eisbn = ''
            eisbnTag = soup.select_one('span.bookInfo__isbn__pdf')
            if eisbnTag:
                identifier_eisbn = ''.join(eisbnTag.stripped_strings)
                if identifier_eisbn.startswith('ISBN (PDF):'):
                    identifier_eisbn = identifier_eisbn.split(':')[-1].strip().replace('-', '').replace(' ', ';')

            date_created = date = ''
            dateTag = soup.select_one('section.copyright')
            if dateTag:
                date = ''.join(dateTag.stripped_strings)
                datelist = re.findall(r'(\d{4})', date)
                if len(datelist) > 0:
                    date = datelist[0]
                else:
                    dateTag = soup.select_one('div.conf-year')
                    if dateTag:
                        date = ''.join(dateTag.stripped_strings)
                date_created = date + '0000'

            subject = ''
            subjectTags = soup.select('section.subjects > a')
            for subjectTag in subjectTags:
                subject = subject + subjectTag.get_text() + ';'
            subject = subject.strip(';')
            creator = ''
            creator_bio = ''
            authorTags = soup.select('div.affiliation-block > div.author')
            for authroTag in authorTags:
                auTag = authroTag.select_one('a')
                if auTag:
                    author = auTag.get_text()
                else:
                    author = authroTag.get_text()
                affTag = authroTag.select_one('footer')
                affiliation = ''
                if affTag:
                    affiliation = ''.join(affTag.stripped_strings)
                creator = creator + author + ';'
                if len(affiliation) > 1:
                    creator_bio = creator_bio + author + ',' + affiliation + ';'
            creator = creator.strip(';').strip()
            creator_bio = creator_bio.strip(';')
            page = ''
            pageTags = soup.select('div.tocList__content > ul > li > div > article > div.tocList__pages')
            if len(pageTags) > 0:
                cnt = len(pageTags) - 1
                while cnt >= 0:
                    page = pageTags[cnt].get_text().split('-')[-1].strip()
                    if page.isdigit():
                        break
                    cnt = cnt - 1
            if not page.isdigit():
                page = ''
            onemessage = (
                lngid, rawid, creator, title, title_series, cover, page, publisher, subject, date, creator_bio,
                date_created, identifier_pisbn, identifier_eisbn, description, identifier_doi, language, country,
                provider, provider_url, provider_id, type_, medium, batch
            )
            return onemessage
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(filename)

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

    def startdown_cover(self, message):
        self.count = 0
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'ascebook')
        cur = conn.cursor()
        cur.execute("select url,cover_url from book where cover_stat=0 and cover_url!=''")
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.detail_path)) > 0:
                self.sendwork('parse_detail')
                return
        for url, cover_url in rows:
            fname = self.cover_path + '/' + url.split('/')[-1] + '.jpg'
            self.sendwork('down_cover', (fname, cover_url))

    def down_cover(self, message):
        fname = message[0]
        cover_url = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_cover', cover_url)
            return
        url = 'https://ascelibrary.org' + cover_url
        while True:
            try:
                proxy = self.getproxy()
                proxies = {'http': proxy, 'https': proxy}
                resp = requests.get(url, proxies=proxies)
            except:
                exMsg = '* ' + traceback.format_exc()
                print(exMsg)
                continue
            if utils.Img2Jpg(resp.content, fname):
                utils.printf('下载图片%s成功' % fname)
                self.senddistributefinish('process_cover', cover_url)
                return

    def process_cover(self, message):
        self.count = self.count + 1
        cover_url = message
        sql = "update book set cover_stat=1 where cover_url='{}'".format(cover_url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 40 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'ascebook')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            self.refreshproxypool()
        if self.totalcount == self.count:
            utils.printf('%s:下载图片完成' % self.provider)
            self.sendwork('parse_detail')

    def startmission(self):
        ConnRabbitMQ = Provider.OpenConnRabbitMQ()
        channel = ConnRabbitMQ.channel()  # 创建频道
        dic = self.package('startdown_html')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneAsceBook = AsceBook('ascebook', 'proxy_asce')

if __name__ == '__main__':
    oneAsceBook.startmission()