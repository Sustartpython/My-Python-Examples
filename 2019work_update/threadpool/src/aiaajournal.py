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

class AiaaJournal(Provider):

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
        self.mapfunc['down_index'] = self.down_index
        self.mapfunc['process_index'] = self.process_index
        self.mapfunc['startdown_index'] = self.startdown_index
        self.mapfunc['parse_index'] = self.parse_index
        self.mapfunc['startdown_cover'] = self.startdown_cover
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['process_cover'] = self.process_cover
        self.mapfunc['mapcover'] = self.mapcover
        self.mapfunc['update'] = self.update
        self.mapfunc['parse_detail'] = self.parse_detail
        self.mapfunc['parse_detail_meta'] = self.parse_detail_meta

    def update(self, message=None):
        self.startdown_html(None)
        # self.startdown_detail(None)
        # self.startdown_cover(None)

    def startdown_html(self, message):
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url1 = 'https://arc.aiaa.org/action/showPublications?pubType=journal&startPage=&ConceptID=111092'
        fname1 = '%s/active.html' % self.html_path
        url2 = 'https://arc.aiaa.org/action/showPublications?pubType=journal&startPage=&ConceptID=111093'
        fname2 = '%s/retired.html' % self.html_path
        self.count = 0
        self.totalcount = 2
        self.sendwork('down_html', (url1,fname1))
        self.sendwork('down_html', (url2,fname2))

    def down_html(self, message):
        url = message[0]
        fname = message[1]
        feature = 'class="search-item clearfix"'
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

    def process_html(self,message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:down_html finish' % self.provider)
            self.sendwork('parse_html')

    def parse_html(self,message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        result = []
        stmt = 'insert ignore into journal(journal_name,url,eissn,cover_url,active) Values(%s,%s,%s,%s,%s)'
        active = 0
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            if filename == 'active.html':
                active = 1
            else:
                active = 0
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            try:
                sel = Selector(text=text)
                for liTag in sel.xpath('//li[@class="search-item clearfix"]'):
                    journal_name = liTag.xpath('./div/h4/a/text()').extract_first().strip()
                    url = liTag.xpath('./div/h4/a/@href').extract_first().replace('journal','loi')
                    eissn = liTag.xpath('./div/div/div/span[@class="meta__eissn"]/text()').extract_first().replace('eISSN: ','').strip()
                    cover_url = liTag.xpath('./div/a/img/@src').extract_first().strip()
                    result.append((journal_name,url,eissn,cover_url,active))
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
        self.senddistributefinish('startdown_index') 

    def startdown_index(self, message):
        utils.printf('%s:开始下载索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select url,active from journal where active=1')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        for url, _ in rows:
            fname = self.index_path + '/' + url.split('/')[-1] + '.html'
            self.sendwork('down_index', (url, fname))

    def down_index(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index')
            return
        feature = 'loi__issue__vol'
        resp = self.gethtml('https://arc.aiaa.org' + url, feature)
        if not resp:
            self.sendwork('down_index', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_index')

    def process_index(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('down_index finish')
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'aiaajournal', 2)
            result = []
            stmt = 'insert ignore into issue(url,stat) Values(%s,%s)'
            cnt = 0
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                sel = Selector(text=text)
                for aTag in sel.xpath('//a[@class="loi__issue__vol"]'):
                    url = aTag.xpath('./@href').extract_first()
                    if url.endswith('/0/0'):
                        continue
                    result.append(('https://arc.aiaa.org'+url,0))
                if utils.parse_results_to_sql(conn, stmt, result, 200):
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
        self.sqlList.clear()
        self.count = 0
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select url,stat from issue where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                self.sendwork('parse_list')
        for url,_ in rows:
            urlsp = url.split('/')
            base_name = '%s_%s.html' % (urlsp[-2],urlsp[-1])
            fdir = '%s/%s' % (self.list_path,urlsp[-3])
            fname = '%s/%s' % (fdir,base_name)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_list', (url,fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list',url)
            return
        feature = 'class="issue-item__title"'
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_list', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list',url)

    def process_list(self, message):
        self.count = self.count + 1
        sql = "update issue set stat=1 where url='{}'".format(message)
        self.sqlList.append(sql)
        if self.count % 40 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'aiaajournal', 2)
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'aiaajournal', 2)
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        result = []
        stmt = 'insert ignore into article(id,url,vol,stat,failcount) Values(%s,%s,%s,%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            sel = Selector(text=text)
            for href in sel.xpath('//h5[@class="issue-item__title"]/a/@href'):
                url = href.extract().replace('/doi/','/doi/abs/').strip()
                id = fullname.split('\\')[-2] + '_' + url.split('/')[-1]
                vol = filename.split('_')[0]
                print(id, url)
                result.append((id, url, vol, 0, 0))
            if utils.parse_results_to_sql(conn, stmt, result,200):
                cnt += len(result)
                utils.printf('%s解析%s条数据到数据库' % (self.provider,cnt))
                result.clear()
        cnt += len(result)
        utils.parse_results_to_sql(conn, stmt, result)
        utils.printf('%s解析%s条数据到数据库' % (self.provider,cnt))
        utils.printf('%s解析列表页完成' % self.provider)
        self.senddistributefinish('startdown_detail')

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select id,url,vol from article where stat=0 and failcount<20')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('parse_detail')
            self.sendwork('startdown_cover')
            return
        for id,url,vol in rows:
            fdir = '%s/%s/%s' % (self.detail_path, id.split('_')[0],vol)
            fname = '%s/%s.html' % (fdir,id.split('_')[-1])
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_detail',(fname,url))


    def down_detail(self, message):
        fname = message[0]
        url = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (url, True))
            return
        feature = 'class="citation__title"'
        resp = self.gethtml('https://arc.aiaa.org'+url, feature)
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
            sql = "update article set stat=1 where url='{}'".format(url)
        else:
            sql = "update article set failcount=failcount+1 where url='{}'".format(url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 50 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'aiaajournal', 2)
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
            self.refreshproxypool()
        if self.totalcount == self.count:
            self.startdown_detail(None)

    def getpageinfo(self):
        if not self.list_path:
            self.initpath()
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            volume = filename.replace('.html', '').split('_')[0]
            issue = filename.replace('.html', '').split('_')[-1]
            sel = Selector(text=text)
            for divTag in sel.xpath('//div[@class="issue-item"]'):
                doi = '10.2514/' + divTag.xpath('./h5/a/@href').extract_first().split('/')[-1]
                page = divTag.xpath('./div[@class="issue-item__pages"]/text()').extract_first().replace('–','-')
                self.dic[doi] = (page,volume,issue)

    def parse_detail(self, message):
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select journal_name,url,pissn,eissn from journal')
        rows = cur.fetchall()
        for journal_name,url,pissn,eissn in rows:
            gch = url.split('/')[-1]
            self.dic[gch] = (journal_name,pissn,eissn)
        cur.close()
        conn.close()
        self.getpageinfo()
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title,identifier_pissn,identifier_eissn,
        description,page,beginpage,endpage,creator_institution,volume,issue,source,publisher, date, date_created, gch,
        language, country,provider,provider_url,identifier_doi, provider_id,type, medium, batch)values(?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname,'zt')
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
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select gch,journal_name,journal_name_en,pissn,eissn from journal')
        rows = cur.fetchall()
        for gch,journal_name,journal_name_en,pissn,eissn in rows:
            self.dic[gch] = (journal_name,journal_name_en,pissn,eissn)
        cur.close()
        conn.close()
        self.predb3('base_obj_meta_a_template_qk.db3','base_obj_meta_a_qk.aiaajournal')
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
        provider = 'aiaajournal'
        type_ = 3
        medium = 2
        batch = time.strftime('%Y%m%d') + '00'
        identifier_doi = '10.2514/' + filename.replace('.html','')
        rawid = identifier_doi
        lngid = 'AIAA_WK_' + rawid
        provider_url = provider + '@' + 'https://arc.aiaa.org/doi/abs/' + identifier_doi
        provider_id = provider + '@' + identifier_doi
        publisher = 'American Institute of Aeronautics and Astronautics'


        date = '1900'
        date_created = '19000000'

        gch = fullname.split('\\')[-3]       

        source,identifier_pissn,identifier_eissn = self.dic[gch]
        page,volume,issue = self.dic[identifier_doi]
        beginpage = page.split('-')[0]
        endpage = page.split('-')[-1]
        gch = provider + "@" + gch


        with open(fullname, encoding='utf8') as f:
            text = f.read()
        sel = Selector(text=text)
        creator = description = creator_institution = ''
        mapmonth = {
                    "January":'0100', "February":'0200', "March":'0300', "April":'0400', "May":'0500', "June":'0600', 
                    "July":'0700', "August":'0800', "September":'0900', "October":'1000',"November":'1100',
                    "December":'1200'}
        try:
            title = sel.xpath('string(//h1[@class="citation__title"])').extract_first().strip()
            # if title == '':
            #     title = sel.xpath('//h1[@class="citation__title"]/text()').extract_first(default='').strip()
            for divTag in sel.xpath('//div[@id="sb-1"]/div/div'):
                author = divTag.xpath('./a/span/text()').extract_first(default='').strip()
                if not author == '': 
                    creator = creator + author + ';'
                organ = divTag.xpath('./div/text()').extract_first(default='').strip()
                if not organ == '': 
                    creator_institution = creator_institution + organ + ';'
            creator = creator.strip(';')
            creator_institution = creator_institution.strip(';')

            dateraw = sel.xpath('//span[@class="current-issue__date"]/a/text()').extract_first(default='').strip()
            
            if not dateraw == '':
                date = dateraw.split(' ')[-1]
                month = dateraw.split(' ')[0]                
                if mapmonth.__contains__(month):
                    date_created = date + mapmonth[month]
            
            description = sel.xpath('//div[@class="abstractSection abstractInFull"]/p/text()').extract_first(
                default='').strip()

            onemessage = (lngid, rawid, creator, title,identifier_pissn,identifier_eissn,
            description,page,beginpage,endpage,creator_institution,volume,issue,
            source,publisher, date, date_created, gch, language, country,provider,provider_url,
            identifier_doi, provider_id,type_, medium, batch)

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
            'accept': '*/*',

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
        conn = utils.init_db('mysql', 'aiaajournal', 2)
        cur = conn.cursor()
        cur.execute('select gch,cover_url from journal')
        rows = cur.fetchall()
        self.count = 0
        self.totalcount = len(rows)
        for gch,cover_url in rows:
            self.sendwork('down_cover', (gch,cover_url))

    def down_cover(self, message):
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        gch = message[0]
        cover_url = message[1]
        filename = self.cover_path + '/' + gch.lower() + '.jpg'
        if os.path.exists(filename):
            self.senddistributefinish('process_cover')
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
            self.senddistributefinish('process_cover')
        else:
            self.sendwork('down_cover',message)
            return

    def process_cover(self, message):
        self.count = self.count + 1
        if self.count == self.totalcount:
            utils.printf('%s:下载图片完成' % self.provider)
            self.sendwork('mapcover')

    def mapcover(self, message):
        nCount = 0
        provider = 'aiaajournal'
        filePath = self.datepath + '/' + provider + '_cover.txt'
        with open(filePath, mode='w', encoding='utf-8') as f:
            for path, dirNames, fileNames in os.walk(self.cover_path):
                for fileName in fileNames:
                    journal = os.path.splitext(fileName)[0].upper()
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


oneAiaaJournal = AiaaJournal('aiaajournal', 'proxy_cnki')

if __name__ == '__main__':
    oneAiaaJournal.startmission()