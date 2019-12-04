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


class YdylInfo(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.dic = {}
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
        self.startdown_html('zx')
        # self.parse_html(None)

    def startdown_html(self, message):
        infotype = message
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url = 'https://www.ydylcn.com/%s/index.shtml' % infotype
        feature = 'class="list-link-1"'
        fdir = '%s/%s' % (self.html_path, infotype)
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        fname = '%s/1.html' % fdir
        utils.printf(fname)
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
        pagetotalnum = sel.xpath('//table//tr/td/text()')[1].re(r'\s*/(.*)页')[0]
        self.count = 0
        self.totalcount = int(pagetotalnum) - 1
        for page in range(2, int(pagetotalnum) + 1):
            self.sendwork('down_html', (page, infotype))

    def down_html(self, message):
        page = message[0]
        infotype = message[1]
        feature = 'class="list-link-1"'
        fname = '%s/%s/%s.html' % (self.html_path, infotype, page)
        if os.path.exists(fname):
            self.senddistributefinish('process_html')
            return
        url = 'https://www.ydylcn.com/%s/index_%s.shtml' % (infotype, page)
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        if infotype == 'zx':
            self.senddistributefinish('process_html', 'zx')
        else:
            self.senddistributefinish('process_html', 'zjgd')

    def process_html(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:down_html %s finish' % (self.provider, message))
            if message == 'zx':
                self.sendwork('startdown_html', 'zjgd')
            else:
                self.sendwork('parse_html')

    def parse_html(self, message):
        if not self.html_path:
            self.initpath()
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'ydylcninfo', 4)
        result = []
        stmt = 'insert ignore into article(article_id,infotype) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            infotype = fullname.split('\\')[-2]
            sel = Selector(text=text)
            for aTag in sel.xpath("//ul[@class='list-link-1']/li/a"):
                article_id = aTag.xpath('./@href').extract_first().split('/')[-1].replace('.shtml', '')
                result.append((article_id, infotype))
                utils.printf(len(result))
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.senddistributefinish('startdown_detail')

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'ydylcninfo', 4)
        cur = conn.cursor()
        cur.execute('select article_id,infotype from article where stat=0 and failcount<10')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('parse_detail')
            return
        for article_id, infotype in rows:
            fdir = '%s/%s' % (self.detail_path, infotype)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_detail', (article_id, infotype))

    def down_detail(self, message):
        article_id = message[0]
        infotype = message[1]
        fname = '%s/%s/%s.html' % (self.detail_path, infotype, article_id)
        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (article_id, infotype, True))
            return
        feature = 'class="tit-g1"'
        url = 'https://www.ydylcn.com/%s/%s.shtml' % (infotype, article_id)
        resp = self.gethtml(url, feature)
        if not resp:
            self.senddistributefinish('process_detail', (article_id, infotype, False))
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_detail', (article_id, infotype, True))

    def process_detail(self, message):
        self.count = self.count + 1
        article_id = message[0]
        infotype = message[1]
        flag = message[2]
        if flag:
            sql = "update article set stat=1 where article_id='{}' and infotype='{}'".format(article_id, infotype)
        else:
            sql = "update article set failcount=failcount+1 where article_id='{}' and infotype='{}'".format(
                article_id, infotype
            )
        self.sqlList.append(sql)
        if len(self.sqlList) >= 50 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'ydylcninfo', 4)
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
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title,
        description, description_source, date, date_created, language, country,
        provider,provider_url, provider_id,type, medium, batch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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
        date = '1900'
        date_created = '19000000'
        infotype = fullname.split('\\')[-2]
        url = 'https://www.ydylcn.com/%s/%s.shtml' % (infotype, rawid)
        provider_url = provider + '@' + url
        provider_id = provider + '@' + rawid
        lngid = utils.GetLngid('00042', rawid)

        with open(fullname, encoding='utf8') as f:
            text = f.read()
        sel = Selector(text=text)
        try:
            title = sel.xpath('//div[@class="tit"]/h1/text()').extract_first()
            creator = description_source = description = ''
            for divstr in sel.xpath('//div[@class="info"]/span/text()').extract():
                utils.printf('divstr:%s' % divstr)
                if divstr.startswith('来源：'):
                    description_source = divstr.replace('来源：', '').replace('、', ';').replace('；', ';').strip()
                elif divstr.startswith('作者：'):
                    creator = divstr.replace('作者：', '').strip().replace(' ', ';').strip(';')
                elif divstr.startswith('发布时间：'):
                    date_created = divstr.replace('发布时间：', '').replace('-', '').strip()
                    if len(date_created) == 6:
                        date_created = date_created + '00'
                    date = date_created[0:4]
            descriptions = sel.xpath("//div[@class='txt']/p[@style='text-align: justify;']/span/text()").extract()
            for item in descriptions:
                description +=item + "\n"

            onemessage = (
                lngid, rawid, creator, title, description, description_source, date, date_created, language, country,
                provider, provider_url, provider_id, type_, medium, batch
            )
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
            'Accept':
                '*/*'
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
        conn = utils.init_db('mysql', 'ydylcninfo', 4)
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
            self.senddistributefinish('process_cover', bookid)
            return
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get(cover_url, headers=HEADER, timeout=20, proxies=proxies)
            # resp = requests.get(cover_url, headers=HEADER, timeout=20)
        except:
            self.sendwork('down_cover', message)
            return
        if utils.Img2Jpg(resp.content, filename):
            utils.printf('下载图片%s成功' % filename)
            self.senddistributefinish('process_cover', bookid)
        else:
            self.sendwork('down_cover', message)
            return

    def process_cover(self, message):
        self.count = self.count + 1
        self.sqlList.append((1, message))
        if self.count % 2 == 1:
            utils.printf('%s:下载成功图片 %s 个' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'ydylcninfo', 4)
            stmt = 'update book set cover_stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'ydylcninfo', 4)
            stmt = 'update book set cover_stat=%s where bookid=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('%s:下载图片完成' % self.provider)

    def mapcover(self, message):
        nCount = 0
        provider = 'ydylcninfo'
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


oneYdylInfo = YdylInfo('ydylcninfo', 'proxy_cnki')

if __name__ == '__main__':
    oneYdylInfo.startmission()