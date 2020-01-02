from Provider import Provider
import utils
import requests
import os
import time
import json
import traceback
import re
from parsel import Selector
from Const import MQQueueFinish

class YdylBook(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.dic = {}
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
        self.mapfunc['update'] = self.update
        self.mapfunc['parse_detail'] = self.parse_detail

    def update(self, message=None):
        self.startdown_html(None)
        # self.startdown_list(None)
        # self.startdown_detail(None)

    def startdown_html(self, message):
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url = 'https://www.ydylcn.com/skwx_ydyl/psbooklist?SiteID=1&PageIndex=1'
        feature = 'class="list-book-1"'
        fname = '%s/%s.html' % (self.html_path, '1')
        if not os.path.exists(fname):
            while True:
                resp = self.gethtml(url, feature)
                if resp:
                    break
            text = resp.content.decode('utf8')
            with open(fname, mode='w', encoding='utf8') as f:
                f.write(text)
        else:
            with open(fname, encoding='utf8') as f:
                text = f.read()
        sel = Selector(text=text)
        pagetotalnum = sel.xpath('//table/tr/td/text()')[10].re(r'\s*/(.*)页')[0]
        self.count = 0
        self.totalcount = int(pagetotalnum) - 1
        for page in range(2,int(pagetotalnum)+1):
            self.sendwork('down_html', page)

    def down_html(self, message):
        page = message
        feature = 'class="list-book-1"'
        fname = '%s/%s.html' % (self.html_path, page)
        if os.path.exists(fname):
            self.senddistributefinish('process_html')
            return
        url = 'https://www.ydylcn.com/skwx_ydyl/psbooklist?SiteID=1&PageIndex=%s' % page
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_html')

    def process_html(self,message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:down_html finish' % self.provider)
            self.sendwork('parse_html')

    def parse_html(self,message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'ydylcnbook', 4)
        result = []
        stmt = 'insert ignore into book(bookid,cover_url) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            sel = Selector(text=text)
            for aTag in sel.xpath('//ul[@class="list-book-1"]/li/a'):
                bookid = aTag.xpath('./@href').extract_first().split('=')[-1]
                cover_url = aTag.xpath('./div/div/img/@src').extract_first()
                result.append((bookid, cover_url))
                utils.printf(len(result))
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.senddistributefinish('startdown_list')                  

    def startdown_list(self, message):
        utils.printf('%s:开始下载列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.refreshproxypool()
        self.sqlList.clear()
        self.count = 0
        conn = utils.init_db('mysql', 'ydylcnbook', 4)
        cur = conn.cursor()
        cur.execute('select bookid,stat from book where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的book不需要更新' % self.provider)
            else:
                # self.sendwork('down_cover')
                self.sendwork('parse_list')
        for bookid,stat in rows:
            self.sendwork('down_list', bookid)

    def down_list(self, message):
        bookid = message
        fname = '%s/%s.html' % (self.list_path,bookid)
        if os.path.exists(fname):
            self.senddistributefinish('process_list', (1,bookid))
            return
        feature = 'class="tit-g1"'
        url = 'https://www.ydylcn.com/skwx_ydyl/bookdetail?SiteID=1&ID=%s' % bookid
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_list', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list', (1,bookid))

    def process_list(self, message):
        self.count = self.count + 1
        self.sqlList.append(message)

        if self.count % 40 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'ydylcnbook', 4)
            stmt = 'update book set stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'ydylcnbook', 4)
            stmt = 'update book set stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.senddistributefinish('startdown_cover')
            # self.sendwork('parse_list')

    def parse_list(self, message):
        conn = utils.init_db('mysql', 'ydylcnbook', 4)
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, title_alternative,
         title_series, cover, subject,identifier_pisbn, description, publisher, date, date_created, language, country,
        provider,provider_url, provider_id, type, medium, batch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        sql = 'insert ignore into article(article_id,stat) Values(%s,%s)'
        count = 0
        articlecnt = 0
        result = []
        for filename, fullname in utils.file_list(self.list_path):
            onemessage, bookdetaillist = self.parse_list_one(filename, fullname)
            if onemessage:
                self.sqlList.append(onemessage)
            if utils.parse_results_to_sql(self.conn, stmt, self.sqlList, 50):
                count += len(self.sqlList)
                utils.printf('%s: 插入 %d 条数据到db3' % (self.provider, count))
                self.sqlList.clear()
            if bookdetaillist:
                for article_id in bookdetaillist:
                    result.append((article_id,0))
                utils.parse_results_to_sql(conn, sql, result)
                articlecnt += len(result)
                result.clear()
                utils.printf('%s: 插入 %d 个文章ID到数据库' % (self.provider, articlecnt))           
        utils.parse_results_to_sql(self.conn, stmt, self.sqlList)
        count += len(self.sqlList)
        utils.printf('%s: 插入 %d 条数据到db3' % (self.provider, count))
        self.conn.close()
        self.conn = None
        utils.msg2weixin('%s: 解析完成,图书成品文件为%s' % (self.provider, self.template_file))
        self.senddistributefinish('startdown_detail')

    def parse_list_one(self, filename, fullname):
        language = 'ZH'
        country = 'CN'
        provider = 'ydylcnbook'
        type_ = 1
        medium = 2
        batch = time.strftime('%Y%m%d') + '00'
        rawid = filename.replace('.html', '')
        publisher = '社会科学文献出版社'
        date = '1900'
        date_created = '19000000'
        url = 'https://www.ydylcn.com/skwx_ydyl/bookdetail?SiteID=1&ID=%s' % rawid
        provider_url = provider + '@' + url
        provider_id = provider + '@' + rawid
        lngid = utils.GetLngid('00041', rawid)
        cover = '/smartlib/ydylcnbook/%s.jpg' % rawid
        cover_path = '%s/%s.jpg' % (self.cover_path,rawid)
        if not os.path.exists(cover_path):
            cover = ''
        with open(fullname, encoding='utf8') as f:
            text = f.read()
        sel = Selector(text=text)
        try:
            title = sel.xpath('//div[@class="tit-g1"]/h3/text()').extract_first()
            creator = title_alternative = identifier_pisbn = title_series = subject = description = ''
            for divTag in sel.xpath('//div[@class="desc"]/p'):
                divstr = divTag.xpath('string(.)').extract_first().strip()
                # utils.printf('divstr:%s' % divstr)
                if divstr.startswith('英 文 名：'):
                    title_alternative = divstr.replace('英 文 名：','').strip()
                elif divstr.startswith('作    者：'):
                    for author in divTag.xpath('./a/text()'):
                        creator = creator + author.extract() + ';'
                    creator = creator.strip(';')
                elif divstr.startswith('I S B N：'):
                    # utils.printf('divstr:%s' % divstr)
                    identifier_pisbn = divstr.replace('I S B N：','').replace('-','').strip()
                    # utils.printf('identifier_pisbn:%s' % identifier_pisbn)
                elif divstr.startswith('丛 书 名：'):
                    title_series = divstr.replace('丛 书 名：','').strip()
                elif divstr.startswith('关键词：'):
                    for keyword in divTag.xpath('./a/text()'):
                        subject = subject + keyword.extract() + ';'
                    subject = subject.strip(';')
            description = sel.xpath('string(//div[@class="item"]/div[@class="txt"])').extract_first().strip()
            description = description.replace('●','').strip()
            onemessage = (lngid, rawid, creator, title, title_alternative, title_series, cover, subject,identifier_pisbn,
            description, publisher, date, date_created, language, country,provider,provider_url, provider_id,
            type_, medium, batch)
            bookdetaillist = []
            for article_id in sel.xpath('//ul[@class="list-article-1"]/li/h5/a/@onclick'):
                pt = re.compile(r'toGeDataBase\((\d+),.*?\)')
                m = pt.match(article_id.extract())
                if m:
                    # utils.printf('文章号%s' % m.group(1))
                    bookdetaillist.append(m.group(1))
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(fullname)
            return False,False

        return onemessage,bookdetaillist

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'ydylcnbook', 4)
        cur = conn.cursor()
        cur.execute('select article_id,stat from article where stat=0 and failcount<20')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('parse_detail')
            return
        for article_id, _ in rows:
            fdir = '%s/%s' % (self.detail_path, article_id[0:3])
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_detail',(fdir,article_id))


    def down_detail(self, message):
        fdir = message[0]
        article_id = message[1]
        fname = '%s/%s.html' % (fdir,article_id)
        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (article_id, True))
            return
        feature = 'class="info"'
        url = 'https://www.ydylcn.com/skwx_ydyl/initDatabaseDetail?siteId=1&contentId=%s&contentType=literature' % article_id
        resp = self.gethtml(url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (article_id, False))
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_detail', (article_id, True))
       


    def process_detail(self, message):
        self.count = self.count + 1
        article_id = message[0]
        flag = message[1]
        if flag:
            sql = "update article set stat=1 where article_id='{}'".format(article_id)
        else:
            sql = "update article set failcount=failcount+1 where article_id='{}'".format(article_id)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 50 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'ydylcnbook', 4)
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
        self.predb3(fname='ydylcninfo_book')
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, title_series, subject,
        subject_en,description,description_en,pagecount, source,creator_bio,publisher, date, date_created, language,
        country,provider,provider_url, provider_id,type, medium, batch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname)
            if onemessage:
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
        language = 'ZH'
        country = 'CN'
        provider = 'ydylcninfo'
        type_ = 14
        medium = 2
        batch = time.strftime('%Y%m%d') + '00'
        rawid = filename.replace('.html', '')
        publisher = '社会科学文献出版社'
        date = '1900'
        date_created = '19000000'
        url = 'https://www.ydylcn.com/skwx_ydyl/initDatabaseDetail?siteId=1&contentId=%s&contentType=literature' % rawid
        provider_url = provider + '@' + url
        provider_id = provider + '@' + rawid
        lngid = utils.GetLngid('00042', rawid)

        with open(fullname, encoding='utf8') as f:
            text = f.read()
        sel = Selector(text=text)
        try:
            title = sel.xpath('//div[@class="info"]/h1/text()').extract_first()
            creator = pagecount = source = title_series = subject = description = subject_en = creator_bio = ''
            for spanTag in sel.xpath('//div[@class="info"]/div[@class="con"]/span'):
                spanstr = spanTag.xpath('string(.)').extract_first().strip()
                # utils.printf('spanstr:%s' % spanstr)
                if spanstr.startswith('作者：'):
                    creator = spanstr.replace('作者：','').strip().replace(' ',';')
                elif spanstr.startswith('出版日期：'):
                    date_created = spanstr.replace('出版日期：','').strip().replace('年','').replace('月','')
                    if len(date_created) == 6:
                        date_created = date_created + '00'
                    date = date_created[0:4]
                elif spanstr.startswith('报告页数：'):
                    pagecount = spanstr.replace('报告页数：','').replace('页','').replace('&nbsp;','').strip()
                elif spanstr.startswith('所属丛书：'):
                    title_series = spanTag.xpath('./a/@title').extract_first()
                elif spanstr.startswith('所属图书：'):
                    source = spanTag.xpath('./a/@title').extract_first()
            description = sel.xpath('string(//div[@class="item"]/div[@class="txt"])').extract_first(default='').strip()
            description_en = sel.xpath('string(//div[@class="item en"]/div[@class="txt"])').extract_first(default='').strip()
            for divTag in sel.xpath('//div[@class="keywords"]'):
                divstr = divTag.xpath('string(.)').extract_first().strip()
                if divstr.startswith('关键词：'):
                    for aTag in divTag.xpath('./a/text()'):
                        subject = subject + aTag.extract().strip() + ';'
                    subject = subject.strip(';')
                elif divstr.startswith('Keywords：'):
                    for aTag in divTag.xpath('./a/text()'):
                        subject_en = subject_en + aTag.extract().strip() + ';'
                    subject_en = subject_en.strip(';')
            for divTag in sel.xpath('//div[@class="desc"]/div/p'):
                divstr = divTag.xpath('string(.)').extract_first().strip()
                if divstr.find('暂无简介') < 0:
                    creator_bio = creator_bio + divstr + '\n'
            creator_bio = creator_bio.strip()
            onemessage = (lngid, rawid, creator, title, title_series, subject, subject_en,description,description_en,
            pagecount, source,creator_bio,publisher, date, date_created, language, country,provider,provider_url,
            provider_id,type_, medium, batch)
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(fullname)
            return False

        return onemessage

    def gethtml(self, url, feature=None, endwith="</html>"):
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
            'Accept': '*/*'

        }
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(url, headers=HEADER, timeout=20, proxies=proxies)
            if resp.status_code != 200:
                print('code !=200')
                return False
            text = resp.content.decode('utf-8')
            if feature:
                if text.find(feature) < 0:
                    print('can not find feature')
                    return False
            if endwith:
                if text.find(endwith) < 0:
                    print('not endwith %s' % endwith)
                    return False
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            return False
        return resp

    def startdown_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'ydylcnbook', 4)
        cur = conn.cursor()
        cur.execute('select bookid,cover_url from book where cover_stat=0')
        rows = cur.fetchall()
        self.count = 0
        self.totalcount = len(rows)
        for bookid, cover_url in rows:
            self.sendwork('down_cover', (bookid, cover_url))

    def down_cover(self, message):
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        bookid = message[0]
        cover_url = message[1]
        filename = self.cover_path + '/' + bookid + '.jpg'
        if os.path.exists(filename):
            self.senddistributefinish('process_cover',bookid)
            return
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(cover_url, headers=HEADER, timeout=20, proxies=proxies)
            # resp = requests.get(cover_url, headers=HEADER, timeout=20)
        except:
            self.sendwork('down_cover',message)
            return
        if utils.Img2Jpg(resp.content, filename):
            utils.printf('下载图片%s成功' % filename)
            self.senddistributefinish('process_cover',bookid)
        else:
            self.sendwork('down_cover',message)
            return

    def process_cover(self, message):
        self.count = self.count + 1
        self.sqlList.append((1,message))
        if self.count % 2 == 1:
            utils.printf('%s:下载成功图片 %s 个' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'ydylcnbook', 4)
            stmt = 'update book set cover_stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'ydylcnbook', 4)
            stmt = 'update book set cover_stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()       
            utils.printf('%s:下载图片完成' % self.provider)
            self.sendwork('parse_list')

    def mapcover(self, message):
        nCount = 0
        provider = 'ydylcnbook'
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


oneYdylBook = YdylBook('ydylcnbook', 'proxy_cnki')

if __name__ == '__main__':
    oneYdylBook.startmission()