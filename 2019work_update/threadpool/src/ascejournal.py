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
import hashlib


def replacedate(day,flag=True):
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
    if flag:
        temp = day.split(',')[0]
        mon = temp.split(' ')[0].strip()
        da = temp.split(' ')[1].strip()
        mon = month[mon]
        if len(da) == 1:
            da = '0' + da
        return day.split(',')[-1].strip() + mon + da
    else:
        mon = day.split(' ')[0].strip()
        mon = month[mon]
        year = day.split(' ')[-1].strip()
        return year + mon + '00'


class AsceJournal(Provider):

    def __init__(self, provider, proxypoolname=None, hdfsroot=None):
        super().__init__(provider, proxypoolname, hdfsroot)
        self.count = 0
        self.totalcount = 0
        self.sqlList = list()
        self.mapfunc['down_index'] = self.down_index
        self.dic = None
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
        self.mapfunc['down_cover'] = self.down_cover
        self.mapfunc['mapcover'] = self.mapcover
        self.mapfunc['startparse_detail'] = self.startparse_detail
        self.mapfunc['parse_detail'] = self.parse_detail
        self.mapfunc['process_parse_detial'] = self.process_parse_detial
        self.mapfunc['merge_detail'] = self.merge_detail
        self.init_dic()

    def init_dic(self):
        if self.dic == None:
            self.dic = {}
            conn = utils.init_db('mysql', 'ascejournal')
            cur = conn.cursor()
            cur.execute('select url,pissn,eissn,journal_name from journal')
            for url,pissn,eissn,journal_name in cur.fetchall():
                gch = url.replace('/loi/','')
                self.dic[gch] = (pissn,eissn,journal_name)
            conn.close()

    def update(self):
        self.startdown_html(None)
        # self.down_cover(None)
        # self.merge_detail(None)
        # self.senddistributefinish('startparse_detail')

    def startdown_html(self, message):
        self.initpath()
        self.refreshproxypool()
        url = 'https://ascelibrary.org/journals?pageSize=50&startPage=0'
        self.sendwork('down_html', url)

    def down_html(self, message):
        url = message
        feature = 'listBody'
        fname = self.html_path + '/html.html'
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
        utils.printf('下载起始页完成')
        self.sendwork('parse_html')

    def parse_html(self, message):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'ascejournal')
        result = []
        stmt = 'insert ignore into journal(url,cover_url,journal_name) Values(%s,%s,%s) on DUPLICATE key UPDATE cover_url=%s'
        cnt = 0
        fname = self.html_path + '/html.html'
        with open(fname, encoding='utf8') as f:
            text = f.read()
        soup = BeautifulSoup(text, 'lxml')
        divlist = soup.select('#frmSearchResults > div > div.listBody > div')
        for divTag in divlist:
            url = divTag.select_one('div.leftSide').a.get('href')
            cover_url = divTag.select_one('div.leftSide').a.img.get('src')
            journal_name = divTag.select_one('div.rightSide > h2').a.get_text()
            result.append((url, cover_url, journal_name,cover_url))
        utils.parse_results_to_sql(conn, stmt, result)
        cnt += len(result)
        utils.printf(cnt)
        conn.close()
        utils.printf('%s:解析起始页完成...' % self.provider)
        self.senddistributefinish('startdown_index')
        self.sendwork('down_cover')

    def startdown_index(self, message):
        utils.printf('%s:开始下载索引页...' % self.provider)
        if not self.index_path:
            self.initpath()
        self.refreshproxypool()
        self.count = 0
        conn = utils.init_db('mysql', 'ascejournal')
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
        feature = 'journalArchiveBackground'
        resp = self.gethtml('https://ascelibrary.org' + url, feature)
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
            self.refreshproxypool()
        if self.count == self.totalcount:
            utils.printf('down_index finish')
            self.sendwork('parse_index')

    def parse_index(self, message):
        try:
            utils.printf('%s:解析索引页开始...' % self.provider)
            conn = utils.init_db('mysql', 'ascejournal')
            result = []
            stmt = 'insert ignore into issue(url,year) Values(%s,%s)'
            cnt = 0
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                soup = BeautifulSoup(text, 'lxml')
                issnTag = soup.select_one('div.issn-header-widget > div')
                pissn = ''
                eissn = ''
                if issnTag:
                    issntext = ''.join(issnTag.stripped_strings)
                for issnstring in issntext.split('|'):
                    issnstring = issnstring.strip()
                    if issnstring.startswith('ISSN (print):'):
                        pissn = issnstring.split(':')[-1].strip()
                    elif issnstring.startswith('ISSN (online):'):
                        eissn = issnstring.split(':')[-1].strip()
                cursor = conn.cursor()
                url = '/loi/%s' % filename.replace('.html', '')
                sql = "update journal set pissn='%s',eissn='%s' where url='%s'" % (pissn,eissn,url)
                cursor.execute(sql)
                conn.commit()

                divTags = soup.select('div.issues > div > div > div')
                for divTag in divTags:
                    aTag = divTag.select_one('a')
                    url = aTag.get('href')
                    spanTag = divTag.find('span',id='loiIssueCoverDateText')
                    year = ''.join(spanTag.stripped_strings)
                    year = re.findall(r'\(.*?(\d{4})\)',year)[0]
                    result.append((url, year))
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
        conn = utils.init_db('mysql', 'ascejournal')
        cur = conn.cursor()
        year = int(time.strftime('%Y'))
        cur.execute('select url,year from issue where stat=0 or year=%s or year=%s' % (year,year-1))
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                self.sendwork('parse_list')
        for url, pubyear in rows:
            fdir = self.list_path + '/' + url.split('/')[-3]
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            fname = fdir + '/' + url.split('/')[-2] + '_' + url.split('/')[-1] + '_' + pubyear + '.html'
            self.sendwork('down_list', (url, fname))

    def down_list(self, message):
        url = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_list', url)
            return
        feature = 'class="articleEntry"'
        resp = self.gethtml('https://ascelibrary.org' + url, feature)
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

        if self.count % 20 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'ascejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'ascejournal')
            stmt = 'update issue set stat=%s where url=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'ascejournal')
        result = []
        stmt = "insert ignore into article(url,vol,issue,gch,num,pubyear) Values(%s,%s,%s,%s,%s,%s) on DUPLICATE key UPDATE pubyear=%s"
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            vol = filename.split('_')[0]
            pubyear = filename.split('_')[-1].replace('.html', '')
            issue = filename.split('_')[-2]
            gch = fullname.split('\\')[-3]
            soup = BeautifulSoup(text, 'lxml')
            gchTag = soup.select_one('div.issues > div > div > div > a')
            gch = gchTag.get('href').split('/')[-3]
            aTags = soup.select('div.art_title.linkable > a')
            if len(aTags) == 0:
                utils.logerror(fullname + '\n')
            num = 0
            for aTag in aTags:
                num += 1
                url = aTag.get('href')
                # if not url.startswith('/'):
                #     continue
                result.append((url, vol, issue, gch, num,pubyear,pubyear))
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
        conn = utils.init_db('mysql', 'ascejournal')
        cur = conn.cursor()
        cur.execute('select url,vol,issue,gch,pubyear from article where stat=0 and failcount<20 limit 10000')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            # self.senddistributefinish('startparse_detail')
            self.sendwork('merge_detail')
            return
        for url, vol, issue, gch, pubyear in rows:
            fdir = self.detail_path + '/' + gch
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_detail', (url, vol, issue, gch,pubyear))

    def down_detail(self, message):
        url = message[0]
        vol = message[1]
        issue = message[2]
        gch = message[3]
        pubyear = message[4]
        num = hashlib.md5(url.encode('utf8')).hexdigest()


        fname = self.detail_path + '/' + gch + '/' + vol + '_' + issue + '_' + num + '.html'

        if os.path.exists(fname):
            self.senddistributefinish('process_detail', (url, True))
            return

        feature = 'publicationContentTitle'
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
            sql = "update article set stat=1 where url='{}'".format(url)
        else:
            sql = "update article set failcount=failcount+1 where url='{}'".format(url)
        self.sqlList.append(sql)
        if len(self.sqlList) >= 200 or (self.totalcount == self.count):
            conn = utils.init_db('mysql', 'ascejournal')
            cur = conn.cursor()
            for sql in self.sqlList:
                cur.execute(sql)
            conn.commit()
            conn.close()
            self.sqlList.clear()
        if self.totalcount == self.count:
            self.startdown_detail(None)

    def startparse_detail(self, message):
        self.predb3()
        self.sqlList.clear()
        self.count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            self.totalcount += 1
            self.senddistributework('parse_detail', (filename, fullname))

    def parse_detail(self, message):
        if not self.datepath:
            self.initpath()

        try:
            filename = message[0]
            fullname = message[1]
            language = 'EN'
            country = 'US'
            provider = 'ascejournal'
            type_ = 3
            medium = 2
            batch = time.strftime('%Y%m%d') + '00'
            volume = filename.split('_')[0]
            issue = filename.split('_')[1]
            gch = fullname.split('\\')[-2]
            identifier_pissn, identifier_eissn,source = self.dic[gch]
            gch = provider + '@' + gch
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            soup = BeautifulSoup(text, 'lxml')
            # fdir = soup.select_one('span.journalNavCenterTd > div > a').get('href').split('/')[-3]
            # fname = fdir + '/' + volume + '_' + issue + '.html'
            
            title = ''
            titleTag = soup.select_one('div.publicationContentTitle')
            if titleTag:
                title = ''.join(titleTag.stripped_strings)
            # source = ''
            # sourceTag = soup.select_one('div.journalMetaTitle.page-heading > h1 > a > span')
            # if sourceTag:
            #     source = ''.join(sourceTag.stripped_strings)
            description = ''
            descriptionTag = soup.select_one('div.NLM_sec.NLM_sec_level_1.hlFld-Abstract > p')
            if descriptionTag:
                description = ''.join(descriptionTag.stripped_strings)
            else:
                descriptionTag = soup.select_one('#texttabcontent > article > div')
                if descriptionTag:
                    for rfTag in descriptionTag.select('div.sec-references'):
                        rfTag.extract()
                    for scTag in descriptionTag.select('script'):
                        scTag.extract()
                    description = ''.join(descriptionTag.stripped_strings)
            identifier_doi = ''
            url = ''
            rawid = ''
            doiTag = soup.select_one('div.publicationContentDoi > a')
            if doiTag:
                doi = doiTag.get('href')
                identifier_doi = doi.replace('https://doi.org/','')
                rawid = identifier_doi
                url = 'https://ascelibrary.org/doi/' + identifier_doi
            provider_url = provider + '@' + url
            provider_id = provider + '@' + rawid
            lngid = utils.GetLngid('00092', rawid)
            # lngid = 'ASCE_WK_' + rawid
            date_created = date = ''
            dateTag = soup.select_one('div.publicationContentEpubDate.dates')
            if dateTag:
                dateTag.select_one('strong').extract()
                date_created = ''.join(dateTag.stripped_strings)
                date_created = replacedate(date_created)
            else:
                dateTag = soup.select_one('div.article-meta-byline > section > div > a')
                if dateTag:
                    date_created = ''.join(dateTag.stripped_strings)
                    date_created = re.findall(r'\((.*?)\)',date_created)[0]
                    date_created = replacedate(date_created,False)
            yearTag = soup.find('div',class_='journalNavTitle')
            if yearTag:
                date = yearTag.a.get_text()
                date = re.findall('(\d{4})',date)[0]

            subject = ''
            subjectTags = soup.select('div.article-meta-byline > section > a')
            for subjectTag in subjectTags:
                subject = subject + subjectTag.get_text() + ';'
            subject = subject.strip(';')
            creator = ''
            creator_bio = ''
            authorTags = soup.select('div.author-block')
            for authroTag in authorTags:
                auTag = authroTag.select_one('div.authorName > a > span > span')
                if auTag:
                    author = auTag.get_text()
                else:
                    auTag = authroTag.select_one('div.authorName')
                    if auTag:
                        author = auTag.get_text()
                affTag = authroTag.select_one('div.authorAffiliation')
                affiliation = ''
                if affTag:
                    affiliation = ''.join(affTag.stripped_strings)
                creator = creator + author + ';'
                if len(affiliation) >1:
                    creator_bio = creator_bio + author + ',' + affiliation + ';'
            creator = creator.strip(';')
            creator_bio = creator_bio.strip(';')
            onemessage = (
                lngid, rawid, creator, title, gch, source, volume, issue, subject, date, creator_bio, date_created,
                identifier_pissn, identifier_eissn, description, identifier_doi, language, country, provider, provider_url,
                provider_id, type_, medium, batch
            )
            self.senddistributefinish('process_parse_detial',onemessage)
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)

    def getissn(self, fname):
        filename = self.list_path + '/' + fname
        with open(filename, encoding='utf8') as f:
            text = f.read()
        soup = BeautifulSoup(text, 'lxml')
        divTag = soup.select_one('div.issn-header-widget > div')
        pissn = ''
        eissn = ''
        if divTag:
            text = ''.join(divTag.stripped_strings)
            for issnstring in text.split('|'):
                issnstring = issnstring.strip()
                if issnstring.startswith('ISSN (print):'):
                    pissn = issnstring.split(':')[-1].strip()
                elif issnstring.startswith('ISSN (online):'):
                    eissn = issnstring.split(':')[-1].strip()
        return (pissn, eissn)

    def process_parse_detial(self, message):
        if not self.conn:
            self.predb3()
        self.count += 1
        self.sqlList.append(message)
        if len(self.sqlList) == 100 or self.count == self.totalcount:
            stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, gch, source, volume, issue, subject, date, creator_bio, date_created,
            identifier_pissn, identifier_eissn, description, identifier_doi, language, country, provider, provider_url,
            provider_id, type, medium, batch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            utils.parse_results_to_sql(self.conn, stmt, self.sqlList)
            utils.printf('%s: 插入 %d 条数据到db3' % (self.provider, self.count))
            self.sqlList.clear()
        if self.count == self.totalcount:
            self.conn.close()
            self.conn = None
            utils.msg2weixin('%s: 解析完成,成品文件为%s' % (self.provider, self.template_file))

    def merge_detail(self,message):
        if not self.datepath:
            self.initpath()
        outPathFile = os.path.join(
            self.merge_path,
            '%s_%d_%d.big_json' % (self.detail_path.split('\\')[-2], os.getpid(), threading.get_ident())
        )
        cnt = 0
        with open(outPathFile, mode='a', encoding='utf-8') as ferge:
            for filename, fullname in utils.file_list(self.detail_path):
                vol = filename.split('_')[0]
                num = filename.split('_')[1]
                journal_raw_id = fullname.split('\\')[-2]
                issn,eissn,journal_name = self.dic[journal_raw_id]
                with open(fullname, encoding='utf8') as f:
                    detail = f.read()
                sumDict = dict()
                sumDict['vol'] = vol
                sumDict['num'] = num
                sumDict['journal_raw_id'] = journal_raw_id
                sumDict['issn'] = issn
                sumDict['eissn'] = eissn
                sumDict['journal_name'] = journal_name
                sumDict['detail'] = detail

                line = json.dumps(sumDict, ensure_ascii=False).strip() + '\n'
                ferge.write(line)                
                cnt += 1
                if cnt % 1000 == 1:
                    print('Write to %s ... %d times' % (outPathFile,cnt))
                    ferge.flush()               
        print('Write to %s ... %d times' % (outPathFile,cnt))      
        flag = utils.ProcAll(self.merge_path, self.hdfs_path)
        if flag:
            utils.msg2weixin('%s:bigjson成功上传至%s' % (self.provider, self.hdfs_path))

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
            return False
        return resp

    def down_cover(self, message):
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'ascejournal')
        cur = conn.cursor()
        cur.execute('select url,cover_url from journal')
        rows = cur.fetchall()
        for url, cover_url in rows:
            import operator
            if operator.eq('/loi/ajrub7',url):
                continue
            filename = self.cover_path + '/' + url.split('/')[-1] + '.jpg'
            if os.path.exists(filename):
                continue

            cover_url = 'https://ascelibrary.org' + cover_url
            while True:
                try:
                    proxy = self.getproxy()
                    proxies = {'http': proxy, 'https': proxy}
                    resp = requests.get(cover_url, proxies=proxies)
                except:
                    continue
                if utils.Img2Jpg(resp.content, filename):
                    utils.printf('下载图片%s成功' % filename)
                    break
        self.sendwork('mapcover')

    def mapcover(self, message):
        nCount = 0
        provider = 'ascejournal'
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
        dic = self.package('startparse_detail')
        if dic:
            task = json.dumps(dic, ensure_ascii=False).encode('utf-8')
            channel.basic_publish(exchange='', routing_key=MQQueueFinish, body=task)


oneAsceJournal = AsceJournal('ascejournal', 'proxy_asce','/RawData/asce/ascejournal/big_json')

if __name__ == '__main__':
    oneAsceJournal.startmission()