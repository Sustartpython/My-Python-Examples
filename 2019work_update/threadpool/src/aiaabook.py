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


class AiaaBook(Provider):

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
        self.mapfunc['update'] = self.update
        self.mapfunc['parse_detail'] = self.parse_detail
        self.mapfunc['parse_detail_meta'] = self.parse_detail_meta

    def update(self, message=None):
        self.startdown_html(None)
        # self.startdown_detail(None)

    def startdown_html(self, message):
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url = 'https://arc.aiaa.org/action/showPublications?pubType=book&startPage=%s&pageSize=100'
        fname = '%s/%s.html'
        self.count = 0
        self.totalcount = 6
        for page in range(1, 7):
            self.sendwork('down_html', (url % page, fname % (self.html_path, page)))

    def down_html(self, message):
        url = message[0]
        fname = message[1]
        feature = 'class="search-item__image"'
        if os.path.exists(fname):
            print(fname)
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
        print(self.count)
        if self.count == self.totalcount:
            utils.printf('%s:down_html finish' % self.provider)
            self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'aiaabook', 2)
        result = []
        stmt = 'insert ignore into book(book_name,url,pub_year,cover_url) Values(%s,%s,%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            try:
                sel = Selector(text=text)
                for liTag in sel.xpath('//li[@class="search-item clearfix"]'):
                    book_name = liTag.xpath('./div/h4/a/text()').extract_first().strip()
                    url = liTag.xpath('./div/h4/a/@href').extract_first()
                    pub_year = liTag.xpath('.//div[@class="search-item__data-group__field meta__date"]/text()'
                                          ).extract_first()
                    cover_url = liTag.xpath('./div/a/img/@src').extract_first().strip()
                    result.append((book_name, url, pub_year, cover_url))
                utils.printf(len(result))
            except:
                exMsg = '* ' + traceback.format_exc()
                print(exMsg)
                utils.logerror(exMsg)
                utils.logerror(fullname)
                return
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
        conn = utils.init_db('mysql', 'aiaabook', 2)
        cur = conn.cursor()
        cur.execute('select url,stat from book where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.detail_path)) == 0:
                utils.logerror('%s:没有新的页面不需要更新' % self.provider)
            else:
                self.sendwork('parse_detail')
            return
        for url, stat in rows:
            fname = '%s/%s.html' % (self.detail_path, url.split('/')[-1])
            self.sendwork('down_detail', (fname, url))

    def down_detail(self, message):
        fname = message[0]
        url = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (url, True))
            return
        feature = 'class="teaser__group-title"'
        resp = self.gethtml('https://arc.aiaa.org' + url, feature)
        if not resp:
            self.sendwork('down_detail', message)
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
        # else:
        #     sql = "update article set failcount=failcount+1 where url='{}'".format(url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 50 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'aiaabook', 2)
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            self.refreshproxypool()
        if self.totalcount == self.count:
            self.senddistributefinish('startdown_cover')

    def parse_detail(self, message):
        conn = utils.init_db('mysql', 'aiaabook', 2)
        cur = conn.cursor()
        cur.execute('select url,pub_year from book')
        rows = cur.fetchall()
        for url, pub_year in rows:
            doi = '10.2514/' + url.split('/')[-1]
            self.dic[doi] = (pub_year)
        cur.close()
        conn.close()
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, identifier_pisbn,
         identifier_eisbn, description, publisher,cover,title_series,
         date,date_created, price, language, country, provider, provider_url, identifier_doi, provider_id,
        type,medium, batch) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname, 'zt')
            # print(onemessage)
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

    def parse_detail_meta(self, message):
        conn = utils.init_db('mysql', 'aiaabook', 2)
        cur = conn.cursor()
        cur.execute('select gch,journal_name,journal_name_en,pissn,eissn from journal')
        rows = cur.fetchall()
        for gch, journal_name, journal_name_en, pissn, eissn in rows:
            self.dic[gch] = (journal_name, journal_name_en, pissn, eissn)
        cur.close()
        conn.close()
        self.predb3('base_obj_meta_a_template_qk.db3', 'base_obj_meta_a_qk.aiaabook')
        self.sqlList.clear()
        stmt = """insert into base_obj_meta_a (author,author_1st,organ,organ_1st,title,title_alt,keyword,pub_year,pub_date,
        vol,num,journal_raw_id,journal_name,journal_name_alt,page_info,begin_page,end_page,subject,is_oa,down_cnt,lngid,
        rawid,product,sub_db,
        provider,sub_db_id,source_type,provider_url,country,language,batch,down_date,publisher,issn,eissn,abstract,
        abstract_alt,doi,fund,ref_cnt,fulltext_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?)"""
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname, 'meta')
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

    def parse_detail_one(self, filename, fullname, db3type):
        language = 'EN'
        country = 'US'
        provider = 'aiaabook'
        type_ = 1
        medium = 2
        batch = time.strftime('%Y%m%d') + '00'
        identifier_doi = '10.2514/' + filename.replace('.html', '')
        rawid = identifier_doi
        lngid = utils.GetLngid('00108', rawid)
        provider_url = provider + '@' + 'https://arc.aiaa.org/doi/book/' + identifier_doi
        provider_id = provider + '@' + identifier_doi
        publisher = 'American Institute of Aeronautics and Astronautics'

        date = '1900'
        date_created = '19000000'

        date = self.dic[identifier_doi]
        date_created = date + '0000'
        cover = ''
        cover_path = '%s/%s.jpg' % (self.cover_path, identifier_doi)
        if os.path.exists(cover_path):
            cover = '/smartlib/aiaabook/%s.jpg' % identifier_doi

        with open(fullname, encoding='utf8') as f:
            text = f.read()
        sel = Selector(text=text)
        creator = description = ''
        try:
            identifier_pisbn = identifier_eisbn = title_series = price = ''
            title = sel.xpath('//h5[@class="teaser__group-title"]/text()').extract_first().strip()
            # if title == '':
            #     title = sel.xpath('//h1[@class="citation__title"]/text()').extract_first(default='').strip()
            creator = sel.xpath('string(//ul[@class="rlist--inline loa mobile-authors"])').extract_first(
                default=''
            ).strip().replace('&nbsp;', ' ').replace(' and ', ';')
            creator = creator.strip(';').replace(',', ';')
            creator = re.sub('\s+;', ';', creator)
            creator = re.sub(';\s+', ';', creator)
            for divTag in sel.xpath('//div[@class="teaser__item"]'):
                divstr = divTag.xpath('./text()').extract_first(default='').strip()
                if divstr.startswith('ISBN (print):'):
                    identifier_pisbn = divstr.replace('ISBN (print):', '').strip().replace('-', '')
                elif divstr.startswith('eISBN:'):
                    identifier_eisbn = divstr.replace('eISBN:', '').strip().replace('-', '')

            title_series = sel.xpath('//head/title/text()').extract_first(default='')
            title_series = title_series.split('|')[-1].strip()

            description = sel.xpath('string(//div[@class="NLM_abstract"])').extract_first(default='').strip()
            if description.startswith('Description'):
                description = description[11:].strip()
            elif description.startswith('About the Book'):
                description = description[14:].strip()
            for divpriceTag in sel.xpath('//div[@class="book-product__content"]'):
                pricelist = divpriceTag.xpath('./div/span[@class="book-product__price__value"]/text()').extract()
                index = len(pricelist)
                if index > 0:
                    price = pricelist[index - 1].strip()
                product_header = divpriceTag.xpath('./h4/text()').extract_first(default='')
                if product_header == 'PDF':
                    break
            onemessage = (
                lngid, rawid, creator, title, identifier_pisbn, identifier_eisbn, description, publisher, cover,
                title_series, date, date_created, price, language, country, provider, provider_url, identifier_doi,
                provider_id, type_, medium, batch
            )

            if db3type == 'zt':
                return onemessage

            # keyword = subject
            # subject = ''
            # journal_raw_id = fullname.split('\\')[-2]
            # for sub_str in sel.xpath('//div[@class="jonListTitle"]/a/text()'):
            #     sub_str = sub_str.extract().strip()
            #     if sub_str == '首页':
            #         continue
            #     subject = subject + sub_str + ';'
            # subject = subject.strip(';')

            # down_cnt = divTag.xpath('./ul/li/span[@class="span02"]/text()').extract_first().replace('下载量:','').strip().replace(',','')
            # fulltext_type = ''
            # pdfTag = divTag.xpath('./ul/li/a[@id="clicknumber"]').extract_first()
            # if pdfTag:
            #     fulltext_type = 'pdf;'
            # htmlTag =  divTag.xpath('./ul/li/span[@id="ctl00_ContentPlaceHolder1_html_show"]/a')
            # if htmlTag:
            #     fulltext_type += 'html;'
            # xmlTag =  divTag.xpath('./ul/li/span[@id="ctl00_ContentPlaceHolder1_xml_show"]/a')
            # if xmlTag:
            #     fulltext_type += 'xml;'
            # fulltext_type = fulltext_type.strip(';')
            # product = 'HANS'
            # sub_db = 'QK'
            # provider = 'HANS'
            # sub_db_id = '00046'
            # provider_url = url
            # batch = time.strftime("%Y%m%d_%H%M%S")
            # down_date = time.strftime("%Y%m%d")
            # down_cnt = down_cnt + '@' + down_date
            # # utils.printf(subject,down_cnt,fulltext_type)
            # refTaglist = divTag.xpath('./div/table/tr/td[@width="45"]')
            # ref_cnt = ''
            # if len(refTaglist) > 0:
            #     ref_cnt = refTaglist[-1].xpath('string(.)').extract_first().strip().replace('[','').replace(']','')

            # onemessage = (creator,author_1st,creator_institution,organ_1st,title,title_alternative,keyword,date,
            # date_created,volume,issue,journal_raw_id,source,source_en,page,beginpage,endpage,subject,is_oa,down_cnt,
            # lngid,rawid,product,sub_db,provider,sub_db_id,type_,provider_url,country,language,batch,down_date,publisher,
            # identifier_pissn,identifier_eissn,description,description_en,identifier_doi,description_fund,ref_cnt,
            # fulltext_type)
            # if db3type == 'meta':
            #     return onemessage
            # else:
            #     return False
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(fullname)
            return False

    def gethtml(self, url, feature=None, endwith="</html>"):
        HEADER = {
            'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
            'accept':
                '*/*',
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
            # exMsg = '* ' + traceback.format_exc()
            # print(exMsg)
            return False
        return resp

    def startdown_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        self.sqlList.clear()
        conn = utils.init_db('mysql', 'aiaabook', 2)
        cur = conn.cursor()
        cur.execute('select url,cover_url from book where cover_stat=0')
        rows = cur.fetchall()
        self.count = 0
        self.totalcount = len(rows)
        fdir = '%s/10.2514' % self.cover_path
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        for url, cover_url in rows:
            self.sendwork('down_cover', (url, cover_url))

    def down_cover(self, message):
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        url = message[0]
        cover_url = message[1]
        filename = '%s/10.2514/%s.jpg' % (self.cover_path, url.split('/')[-1])
        if os.path.exists(filename):
            self.senddistributefinish('process_cover', url)
            return
        try:
            proxy = self.getproxy()
            proxies = {'http': proxy, 'https': proxy}
            resp = requests.get('https://arc.aiaa.org' + cover_url, headers=HEADER, timeout=20, proxies=proxies)
            # resp = requests.get(cover_url, headers=HEADER, timeout=20)
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            self.sendwork('down_cover', message)
            return
        if utils.Img2Jpg(resp.content, filename):
            utils.printf('下载图片%s成功' % filename)
            self.senddistributefinish('process_cover', url)
        else:
            self.sendwork('down_cover', message)
            return

    def process_cover(self, message):
        self.count = self.count + 1
        sql = "update book set cover_stat=1 where url='{}'".format(message)
        self.sqlList.append(sql)
        if self.totalcount == self.count:
            conn = utils.init_db('mysql', 'aiaabook', 2)
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            utils.printf('%s:下载图片完成' % self.provider)
            self.sendwork('parse_detail')

    def startmission(self):
        ConnRabbitMQ = Provider.OpenConnRabbitMQ()
        channel = ConnRabbitMQ.channel()  # 创建频道
        dic = self.package('startdown_list')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneAiaaBook = AiaaBook('aiaabook', 'proxy_cnki')

if __name__ == '__main__':
    oneAiaaBook.startmission()