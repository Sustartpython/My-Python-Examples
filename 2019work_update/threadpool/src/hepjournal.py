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
from bs4 import BeautifulSoup,NavigableString
from parsel import Selector
from Const import MQQueueFinish


def replacedate(day):
    month = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12"
    }

    da = day.split(' ')[0]
    mon = day.split(' ')[1].strip()
    year = day.split(' ')[2].strip()
    mon = month[mon]
    if len(da) == 1:
        da = '0' + da
    return year + mon + da


class HepJournal(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.dic = {}
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
        self.mapfunc['parse_html'] = self.parse_html
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['mapcover'] = self.mapcover
        self.mapfunc['update'] = self.update
        self.mapfunc['startdown_engin'] = self.startdown_engin
        self.mapfunc['down_engin'] = self.down_engin
        self.mapfunc['parse_detail'] = self.parse_detail

    def update(self, message=None):
        self.startdown_html(None)

    def startdown_html(self, message):
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url = 'http://journal.hep.com.cn/hep/EN/column/column7265.shtml'
        self.sendwork('down_html', url)

    def down_html(self, message):
        url = message
        feature = 'cae_journal_link'
        fname = '%s/%s.html' % (self.html_path, 'start')
        if os.path.exists(fname):
            self.parse_html()
            return
        resp = self.gethtml(url, feature)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.parse_html()

    def parse_html(self):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'hepjournal', 4)
        result = []
        stmt = 'insert ignore into journal(journal_id,journal_name,url,cover_url) Values(%s,%s,%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            sel = Selector(text=text)
            for tdTag in sel.xpath('//tr/td[contains(@height, "101")]'):
                cover_url = tdTag.xpath('./div[1]/img/@src').extract_first()
                if cover_url == None:
                    cover_url = tdTag.xpath('./div[1]/span/img/@src').extract_first()
                name = tdTag.xpath('string(./div[2]/a)').extract_first()
                if name == 'Engineering':
                    continue
                url = tdTag.xpath('./div[2]/a/@href').extract_first()
                if url == None:
                    name = tdTag.xpath('./div[2]/span/a/text()').extract_first()
                    url = tdTag.xpath('./div[2]/span/a/@href').extract_first()
                gch = url.split('/')[-4]
                url = 'http://journal.hep.com.cn/%s/EN/article/showVolumnList.do' % gch
                result.append((gch, name, url, cover_url))
            for liTag in sel.xpath('//tr/td[contains(@align, "center")]/ul/li/a'):
                cover_url = liTag.xpath('./img/@src').extract_first()
                url = liTag.xpath('./@href').extract_first()
                gch = url.split('/')[-1]
                url = 'http://journal.hep.com.cn/%s/EN/article/showVolumnList.do' % gch
                result.append((gch, '', url, cover_url))

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
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,url from journal where journal_id!="engi"')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        for journal_id, url in rows:
            fname = self.index_path + '/' + journal_id + '.html'
            self.sendwork('down_index', (url, fname))

    def down_index(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index')
            return
        feature = 'J_WenZhang'
        resp = self.gethtml(url, feature)
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
            utils.printf('%s:down_index finish' % self.provider)
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'hepjournal', 4)
            self.sqlList.clear()
            cur = conn.cursor()
            ptpissn = re.compile(r'ISSN (\w{4}-\w{4}) \(Print\)')
            pteissn = re.compile(r'ISSN (\w{4}-\w{4}) \(Online\)')
            ptpissn1 = re.compile(r'ISSN Print: (\w{4}-\w{4})')
            pteissn1 = re.compile(r'ISSN Print: (\w{4}-\w{4})')
            ptpissn2 = re.compile(r'ISSN (\w{4}-\w{4})')
            ptcn = re.compile(r'CN.*(\w{2}-\w{4}/\w{1,2})')
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                sel = Selector(text=text)
                gch = filename.replace('.html', '')
                for aTag in sel.xpath('//tr/td/a[@class="J_WenZhang"]'):
                    url = 'http://journal.hep.com.cn/%s/EN' % gch + aTag.xpath('./@href').extract_first().replace(
                        '..', ''
                    )
                    sql = "insert ignore into issue(url,journal_id) Values('%s','%s')" % (url, gch)
                    cur.execute(sql)

                conn.commit()
                issnstring = sel.xpath('string(//div[@class="issn"])').extract_first()
                if not issnstring:
                    issnstring = sel.xpath('string(//div[@style="background:#fcefcd;"]/div)').extract_first()
                if not issnstring:
                    issnstring = sel.xpath('string(//td[@style="font-size: 15px"])').extract_first()
                pissn = eissn = cnno = ''
                pissn = ptpissn.findall(issnstring)
                if pissn:
                    pissn = pissn[0]
                else:
                    pissn = ptpissn1.findall(issnstring)
                    if pissn:
                        pissn = pissn[0]
                    else:
                        pissn = ptpissn2.findall(issnstring)
                        if pissn:
                            pissn = pissn[0]
                        else:
                            pissn = ''
                eissn = pteissn.findall(issnstring)
                if eissn:
                    eissn = eissn[0]
                else:
                    eissn = pteissn1.findall(issnstring)
                    if eissn:
                        eissn = eissn[0]
                    else:
                        eissn = ''
                cnno = ptcn.findall(issnstring)
                if cnno:
                    cnno = cnno[0]
                else:
                    cnno = ''
                sql = 'update journal set issn="%s",eissn="%s",cnno="%s" where journal_id="%s"' % (
                    pissn, eissn, cnno, gch
                )
                cur.execute(sql)
                conn.commit()

            cur.close()
            conn.close()
            utils.printf('%s:解析索引页完成...' % self.provider)
            # self.sendwork('down_cover')
            self.senddistributefinish('startdown_list')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def startdown_engin(self, message):
        utils.printf('%s:开始下载engin索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.sendwork('down_engin')

    def down_engin(self, message):
        url = 'http://engineering.org.cn/EN/2095-8099/current.shtml'
        feature = 'txt_biaoti'
        fdir = self.list_path + '/' + 'engi'
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        fname = '%s/engi_current.html' % fdir
        while True:
            resp = self.gethtml(url, feature)
            if resp:
                break
        selcover = Selector(text=resp.content.decode('utf8'))
        tdTag = selcover.xpath('//td[@class="img_display"]')[0]
        engicoverurl = tdTag.xpath('./img/@src').extract_first()
        engicoverurl = engicoverurl.replace('../..','')
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        url = 'http://engineering.org.cn/EN/article/showOldVolumnList.do'
        feature = 'Current Issue'
        while True:
            resp = self.gethtml(url, feature)
            if resp:
                break
        sel = Selector(text=resp.content.decode('utf8'))
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute("update journal set cover_url='%s' where journal_id='engi'" % engicoverurl)
        for aTag in sel.xpath('//a[contains(@href, "../volumn/volumn")]'):
            url = 'http://engineering.org.cn/EN' + aTag.xpath('./@href').extract_first().replace('..', '')
            sql = "insert ignore into issue(url,journal_id) Values('%s','%s')" % (url, 'engi')
            cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        self.senddistributefinish('startdown_list')

    def startdown_list(self, message):
        utils.printf('%s:开始下载列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.refreshproxypool()
        self.sqlList.clear()
        self.count = 0
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute('select url,journal_id from issue where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                # self.sendwork('down_cover')
                self.sendwork('parse_list')
        for url, journal_id in rows:
            fdir = self.list_path + '/' + journal_id
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            fname = fdir + '/' + journal_id + '_' + url.split('/')[-2] + '_' + url.split('/')[-1].replace(
                '.shtml', '.html'
            )
            self.sendwork('down_list', (url, fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list', url)
            return
        feature = 'txt_biaoti'
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
            conn = utils.init_db('mysql', 'hepjournal', 4)
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'hepjournal', 4)
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'hepjournal', 4)
        result = []
        stmt = 'insert ignore into article(url,journal_id) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            journal_id = filename.split('_')[0]
            sel = Selector(text=text)
            for aTag in sel.xpath('//a[@class="txt_biaoti"]'):
                url = aTag.xpath('./@href').extract_first()
                result.append((url, journal_id))
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
        self.sendwork('down_cover')

    def startdown_detail(self, message):
        if not self.detail_path:
            self.initpath()
        self.sqlList.clear()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute('select url,journal_id from article where stat=0 and failcount<20')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            self.sendwork('parse_detail')
            return
        for url, journal_id in rows:
            fdir = '%s/%s' % (self.detail_path, journal_id)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_detail', (url, journal_id))

    def down_detail(self, message):
        url = message[0]
        journal_id = message[1]
        fname = '%s/%s/%s_%s.html' % (self.detail_path, journal_id, url.split('/')[-2], url.split('/')[-1])
        if journal_id == 'laf' and not url.split('/')[-2].startswith('10.'):
            fname = '%s/%s/%s_%s_%s.html' % (
                self.detail_path, journal_id, url.split('/')[-3], url.split('/')[-2], url.split('/')[-1]
            )
        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (url, True))
            return
        feature = 'J_biaoti_en'
        resp = self.gethtml(url, feature)
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
        if len(self.sqlList) >= 200 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'hepjournal', 4)
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
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,journal_name,issn,eissn,cnno from journal')
        rows = cur.fetchall()
        for journal_id, journal_name, issn, eissn, cnno in rows:
            self.dic[journal_id] = (journal_name, issn, eissn, cnno)
        cur.close()
        conn.close()
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, volume, issue, page, beginpage,
        endpage, publisher, subject, date,creator_institution, date_created, source, identifier_pissn, identifier_eissn,
        identifier_cnno, description, identifier_doi, language, country, provider, provider_url, provider_id, type, medium,
        batch, gch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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
        try:
            language = 'EN'
            country = 'CN'
            provider = 'hepjournal'
            type_ = 3
            medium = 2
            batch = time.strftime('%Y%m%d') + '00'
            identifier_doi = filename.replace('.html', '').replace('_', '/')
            rawid = identifier_doi
            gch = fullname.split('\\')[-2]
            source, identifier_pissn, identifier_eissn, identifier_cnno = self.dic[gch]
            publisher = 'Higher Education Press'
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            url = ''
            if gch == 'engi':
                url = 'http://engineering.org.cn/EN/%s' % rawid
            elif gch == 'laf' and not rawid.split('/')[0].startswith('10.'):
                identifier_doi = ''
                urlTag = soup.select_one('meta[name*="HW.ad-path"]')
                url = urlTag.get('content').strip()
                rawid = url.replace('http://journal.hep.com.cn/laf/EN/','')
            else:
                url = 'http://journal.hep.com.cn/%s/EN/%s' % (gch, rawid)
            provider_url = provider + '@' + url
            provider_id = provider + '@' + rawid
            gch = provider + "@" + gch
            lngid = utils.GetLngid('00025', rawid)

            title = ''
            titleTag = soup.select_one('div.J_biaoti_en')
            if titleTag:
                title = ''.join(titleTag.stripped_strings)

            description = ''.strip()
            for tdTag in soup.select('td[class="J_zhaiyao"]'):
                if tdTag.select_one('p'):
                    description = ''.join(tdTag.p.stripped_strings)
                    break
                bTag = tdTag.select_one('b')
                if bTag:
                    if bTag.get_text() == 'Abstract':
                        description = ''.join(tdTag.stripped_strings).replace('Abstract','')
                        break
            date_created = date = ''
            dateTag = soup.select_one('meta[name*="DC.Date"]')
            if dateTag:
                date_created = dateTag.get('content').replace('-', '')
            else:
                date_created = '19000000'
            if date_created == '':
                for spanTag in soup.select('span[class*="J_zhaiyao"]'):
                    strspan = ''.join(spanTag.stripped_strings)
                    if strspan.startswith('Online First Date:'):
                        date_created = strspan.replace('Online First Date:', '').strip()
                        date_created = replacedate(date_created)
                        break

            date = date_created[:4]

            subject = ''
            subjectTag = soup.select_one('meta[name*="keywords"]')
            if subjectTag:
                subject = subjectTag.get('content').replace(',', ';').strip(';')
                subject = re.sub(r'</?\w+[^>]*>','',subject)
                if subject == '&nbsp':
                    subject = ''

            beginpage = ''
            beginpageTag = soup.select_one('meta[name*="citation_firstpage"]')
            if beginpageTag:
                beginpage = beginpageTag.get('content').strip()

            endpage = ''
            endpageTag = soup.select_one('meta[name*="citation_lastpage"]')
            if endpageTag:
                endpage = endpageTag.get('content').strip()

            if endpage == '':
                endpage = beginpage

            page = ''
            if not beginpage == '':
                page = beginpage + '-' + endpage

            volume = ''
            volumeTag = soup.select_one('meta[name*="citation_volume"]')
            if volumeTag:
                volume = volumeTag.get('content').strip()

            issue = ''
            issueTag = soup.select_one('meta[name*="citation_issue"]')
            if issueTag:
                issue = issueTag.get('content').strip()

            creator = ''
            authorTag = soup.select_one('td[class="J_author_EN"]')
            if authorTag:
                if authorTag.select_one('sup'):
                    supTags = authorTag.select('sup')
                    suplist = []
                    tmpsup = ''
                    cnt = 0
                    authorlist = []
                    for supTag in supTags:
                        strsup = supTag.get_text()                        
                        tmpsup += strsup
                        cnt += 1
                        nextTag = supTag.next_sibling
                        beforeTag = supTag.previous_sibling
                        if isinstance(beforeTag, NavigableString):
                            author = beforeTag.replace('(', '').replace(')', '').strip().strip(',')
                            if not author == '':
                                authorlist.append(author)

                        # supTag.extract()
                        if isinstance(nextTag, NavigableString):
                            if nextTag == '(':
                                suplist.append(tmpsup.strip(','))
                                tmpsup = ''
                                continue
                        if not tmpsup.endswith(','):
                            suplist.append(tmpsup)
                            tmpsup = ''
                        elif cnt == len(supTags):
                            suplist.append(tmpsup.strip(','))
                    # tmpauthor = ''.join(authorTag.stripped_strings)
                    # tmpauthor = tmpauthor.replace('(', '').replace(')', '').strip().strip(',')
                    # authorlist = tmpauthor.split(',')

                    if len(authorlist) == len(suplist):
                        for i in range(len(authorlist)):
                            creator = creator + '%s[%s];' % (authorlist[i], suplist[i])
                    elif len(authorlist) == len(supTags):
                        for i in range(len(authorlist)):
                            creator = creator + '%s[%s];' % (authorlist[i], supTags[i].get_text().strip(','))
                    # print(authorlist)
                    # print(suplist)
                    if creator == '':
                        for i in range(len(authorlist)):
                            if len(authorlist) < len(suplist):
                                creator = creator + '%s[%s];' % (authorlist[i], suplist[i])
                            else:
                                creator = creator + authorlist[i] + ';'
                    creator = creator.strip(';')
                    
                else:
                    creator = ''.join(authorTag.stripped_strings)
                    creator = creator.replace('(', '').replace(')', '').replace(',', ';').strip()
            creator = re.sub(r';\s*',';',creator)
            insTag = soup.select_one('td[class="J_author"]')
            creator_institution = ''
            if insTag:
                for brTag in insTag.select('br'):
                    brTag.insert_after(soup.new_string(";"))
                affiliation = ''.join(insTag.stripped_strings)
                affiliation = re.sub(r'\n','',affiliation)
                for ins in affiliation.split(';'):
                    ins = ins.strip()
                    ptins = re.compile('(\w{1,2})\.\s*(.*)')
                    m = ptins.match(ins)
                    if m:
                        creator_institution = creator_institution + '[%s]%s;' % (m.group(1), m.group(2))
                if creator_institution == '':
                    creator_institution = affiliation
                creator_institution = creator_institution.strip(';')

            onemessage = (
                lngid, rawid, creator, title, volume, issue, page, beginpage, endpage, publisher, subject, date,
                creator_institution, date_created, source, identifier_pissn, identifier_eissn, identifier_cnno,
                description, identifier_doi, language, country, provider, provider_url, provider_id, type_, medium,
                batch, gch
            )
            return onemessage
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(fullname)
            return False

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
            text = resp.content.decode('utf-8')
            if feature:
                if text.find(feature) < 0:
                    print('can not find feature')
                    return False
            if text.find('</html>') < 0 and text.find('</HTML>') < 0:
                print('not endwith </html>')
                return False
        except:
            return False
        return resp

    def down_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'hepjournal', 4)
        cur = conn.cursor()
        cur.execute("select journal_id,cover_url from journal where cover_url!=''")
        rows = cur.fetchall()
        HEADER = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        for journal_id, cover_url in rows:
            filename = self.cover_path + '/' + journal_id + '.jpg'
            if os.path.exists(filename):
                continue
            if journal_id == 'engi':
                continue
                # cover_url = 'http://engineering.org.cn' + cover_url
            else:               
                cover_url = 'http://journal.hep.com.cn' + cover_url
            while True:
                try:
                    proxy = self.getproxy()
                    proxies = {'http': proxy, 'https': proxy}
                    resp = requests.get(cover_url, headers=HEADER, timeout=20, proxies=proxies)
                    # resp = requests.get(cover_url, headers=HEADER, timeout=20)
                except:
                    utils.printf(filename)
                    continue
                if utils.Img2Jpg(resp.content, filename):
                    utils.printf('下载图片%s成功' % filename)
                    break
        self.sendwork('mapcover')

    def mapcover(self, message):
        nCount = 0
        provider = 'hepjournal'
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


oneHepJournal = HepJournal('hepjournal', 'proxy_cnki')

if __name__ == '__main__':
    oneHepJournal.startmission()