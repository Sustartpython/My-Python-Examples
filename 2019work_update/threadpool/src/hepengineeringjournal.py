from Provider import Provider
import utils
import requests
import os
import time
import json
import traceback
import re
from bs4 import BeautifulSoup,NavigableString
from parsel import Selector
from Const import MQQueueFinish
from fake_useragent import UserAgent
import html


def replacedate(day):
    # month = {
    #     "January": "01",
    #     "February": "02",
    #     "March": "03",
    #     "April": "04",
    #     "May": "05",
    #     "June": "06",
    #     "July": "07",
    #     "August": "08",
    #     "September": "09",
    #     "October": "10",
    #     "November": "11",
    #     "December": "12"
    # }

    month = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12"
    }

    da = day.split(' ')[0]
    mon = day.split(' ')[1].strip()
    year = day.split(' ')[2].strip()
    mon = month[mon]
    if len(da) == 1:
        da = '0' + da
    return year + mon + da


class HepEngineeringJournal(Provider):

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
        self.mapfunc['parse_detail'] = self.parse_detail
        self.mapfunc['get_issuelist'] = self.get_issuelist
        self.mapfunc['parse_detail_meta'] = self.parse_detail_meta

    def update(self, message=None):
        self.startdown_html(None)
        # self.startdown_list(None)

    def startdown_html(self, message):
        if not self.html_path:
            self.initpath()
        self.refreshproxypool()
        url = 'http://www.engineering.org.cn/default/journals/loadJournals?width=168&height=225'
        self.sendwork('down_html', url)

    def down_html(self, message):
        url = message
        feature = '"success":true'
        fname = '%s/%s.json' % (self.html_path, 'start')
        if os.path.exists(fname):
            self.parse_html()
            return
        resp = self.gethtml(url, feature,None)
        if not resp:
            self.sendwork('down_html', message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.parse_html()

    def parse_html(self):
        utils.printf('%s:解析起始页开始...' % self.provider)
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        result = []
        stmt = 'insert ignore into journal(journal_id,journal_name,cover_url) Values(%s,%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.html_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            try:
                dic = json.loads(text, encoding='utf8')
                for dicitem in dic['resultValue']:
                    dicitem = json.loads(dicitem, encoding='utf8')
                    gch = dicitem['id']
                    name = dicitem['name']
                    cover_url = dicitem['volumeImg']
                    if cover_url == '':
                        cover_url = dicitem['journalImg']
                    print(gch, name, cover_url)
                    result.append((gch, name, cover_url))
            except:
                exMsg = '* ' + traceback.format_exc()
                print(exMsg)
                utils.logerror(exMsg)


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
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,cover_url from journal')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        for journal_id, _ in rows:
            fname = self.index_path + '/' + journal_id + '.json'
            self.sendwork('down_index', (journal_id, fname))

    def down_index(self, message):
        journal_id = message[0]
        fname = message[1]
        if os.path.exists(fname):
            self.senddistributefinish('process_index')
            return
        feature = '"success":true'
        url = "http://www.engineering.org.cn/default/journalIndex/queryJournalById?journalId=%s" % journal_id
        resp = self.gethtml(url, feature, None)
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
            conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
            self.sqlList.clear()
            cur = conn.cursor()
            for filename, fullname in utils.file_list(self.index_path):
                with open(fullname, encoding='utf8') as f:
                    text = f.read()
                dic = json.loads(text,encoding='utf-8')
                gch = filename.replace('.json', '')
                dicitem = dic['resultValue']
                issn = dicitem['issnNm']
                cnno = dicitem['cnNm']                
                sql = 'update journal set issn="%s",cnno="%s" where journal_id="%s"' % (
                    issn, cnno, gch
                )
                cur.execute(sql)
                conn.commit()

            cur.close()
            conn.close()
            utils.printf('%s:解析索引页完成...' % self.provider)
            # self.sendwork('down_cover')
            self.senddistributefinish('get_issuelist')
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
    
    def get_issuelist(self,message):
        utils.printf('%s:开始获取期列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.refreshproxypool()
        self.sqlList.clear()
        self.count = 0
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,journal_name from journal')
        rows = cur.fetchall()
        utils.printf(rows)
        for journal_id,name in rows:
            text = None
            while True:
                url = 'http://www.engineering.org.cn/default/journal/CurrentIssue/AllVolumeId?journalId=%s' % journal_id
                utils.printf(url)
                resp = self.gethtml(url,'"success":true',None)
                if resp:
                    text = resp.content.decode('utf8')
                    break
            dic = json.loads(text,encoding='utf-8')
            index = 1
            for issue_id in dic['resultValue']:
                sql = 'insert into issue(journal_id,issue_id,issue_index) Values(%s,%s,%s) on DUPLICATE key UPDATE issue_index=%s' % (journal_id,issue_id,index,index)
                cur.execute(sql)
                index += 1           
            conn.commit()
            utils.printf('%s:插入%s期' % (self.provider,index))
        conn.close()
        self.senddistributefinish('startdown_list')
            
        


    def startdown_list(self, message):
        utils.printf('%s:开始下载列表页...' % self.provider)
        if not self.list_path:
            self.initpath()
        self.refreshproxypool()
        self.sqlList.clear()
        self.count = 0
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,issue_id,issue_index from issue where stat=0')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            if len(os.listdir(self.list_path)) == 0:
                utils.logerror('%s:没有新的issue不需要更新' % self.provider)
            else:
                # self.sendwork('down_cover')
                self.sendwork('parse_list')
        for journal_id,issue_id,issue_index in rows:
            fdir = self.list_path + '/' + journal_id
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            self.sendwork('down_list', (journal_id,issue_id,issue_index))

    def down_list(self, message):
        journal_id = message[0]
        issue_id = message[1]
        issue_index = message[2]
        fname = '%s/%s/%s.json' % (self.list_path,journal_id,issue_id)
        if os.path.exists(fname):
            self.senddistributefinish('process_list', (1,journal_id,issue_id))
            return
        feature = '"success":true'
        url = 'http://www.engineering.org.cn/default/journal/CurrentIssue/getContents?journalId=%s&pageIndex=1&pageSize=%s' % (journal_id,issue_index)
        resp = self.gethtml(url, feature, None)
        if not resp:
            self.sendwork('down_list', message)
            utils.printf(message)
            return
        with open(fname, mode='w', encoding='utf8') as f:
            f.write(resp.content.decode('utf8'))
        utils.printf('下载 %s 成功' % fname)
        self.senddistributefinish('process_list', (1,journal_id,issue_id))

    def process_list(self, message):
        self.count = self.count + 1
        self.sqlList.append(message)

        if self.count % 40 == 1:
            utils.printf('%s:下载成功 %s 页' % (self.provider, self.count))
            conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
            stmt = 'update issue set stat=%s where journal_id=%s and issue_id=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            conn.close()
        if self.count % 100 == 0:
            self.refreshproxypool()
        if self.count == self.totalcount:
            conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
            stmt = 'update issue set stat=%s where journal_id=%s and issue_id=%s'
            utils.parse_results_to_sql(conn, stmt, self.sqlList)
            self.sqlList.clear()
            utils.printf('downloadlist finish')
            self.sendwork('parse_list')

    def parse_list(self, message):
        utils.printf('%s:解析列表页开始...' % self.provider)
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        result = []
        stmt = 'insert ignore into article(article_id,journal_id) Values(%s,%s)'
        cnt = 0
        for filename, fullname in utils.file_list(self.list_path):
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            journal_id = fullname.split('\\')[-2]
            dicitem = json.loads(text,encoding='utf-8')['resultValue']
            for lanmu in dicitem.keys():
                for fenlei in dicitem[lanmu].keys():
                    for dicdetail in dicitem[lanmu][fenlei]:
                        article_id = dicdetail['id']
                        result.append((article_id,journal_id))
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
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select article_id,journal_id from article where stat=0 and failcount<3')
        rows = cur.fetchall()
        self.totalcount = len(rows)
        if self.totalcount == 0:
            utils.printf('%s:下载详情页完成' % self.provider)
            # self.sendwork('parse_detail_meta')
            self.sendwork('parse_detail')
            # self.sendwork('down_cover')
            return
        messagelist = []
        for article_id, journal_id in rows:
            fdir = '%s/%s' % (self.detail_path, journal_id)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            messagelist.append((article_id, journal_id))
            if len(messagelist) == 30:
                blist = messagelist.copy()
                self.sendwork('down_detail', blist)
                # utils.printf('a'+len(messagelist))
                # utils.printf(messagelist)
                messagelist.clear()
        if len(messagelist) > 0:
            self.sendwork('down_detail', messagelist)
            # utils.printf('b'+len(messagelist))
            # utils.printf(messagelist)


    def down_detail(self, message):
        headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Accept':
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Host': 'www.engineering.org.cn',
            'Connection': 'keep-alive',

        }
        proxy = self.getproxy()
        proxies = {'http': proxy, 'https': proxy}
        feature = '"success":true'
        # utils.printf(message)
        # ua = UserAgent(use_cache_server=False)
        # headers['User-Agent'] = ua.random
        
        for article_id, journal_id in message:
            utils.printf(article_id,journal_id)
            fname = '%s/%s/%s_%s.json' % (self.detail_path, journal_id, journal_id, article_id)
            if os.path.exists(fname):
                self.senddistributefinish('process_detail', (article_id, True))
                utils.printf('exsit %s' % fname)
                continue       
            url = 'http://www.engineering.org.cn/default/journalDetails/loadJournalDetails?articleId=%s&behavior=' % article_id
            try:
                resp = requests.get(url, headers=headers,timeout=30, proxies=proxies)
                utils.printf(11111111111111111111111111111111111)
            except:
                self.senddistributefinish('process_detail', (article_id, False))
                proxy = self.getproxy()
                proxies = {'http': proxy, 'https': proxy}
                # headers['User-Agent'] = ua.random
                utils.printf('change proxy')
                utils.printf('暂停5S')
                time.sleep(5)
                continue
            if resp.status_code != 200:
                print('code !=200')
                print(resp.content.decode('utf-8'))
                utils.logerror(resp.content.decode('utf-8'))
                self.senddistributefinish('process_detail', (article_id, False))
                proxy = self.getproxy()
                proxies = {'http': proxy, 'https': proxy}
                # headers['User-Agent'] = ua.random
                utils.printf('change proxy1')
                utils.printf('暂停5S')
                time.sleep(5)
                continue
            text = resp.content.decode('utf-8')
            if text.find(feature) < 0:
                print('can not find feature')
                print(text)
                self.senddistributefinish('process_detail', (article_id, False))
                proxy = self.getproxy()
                proxies = {'http': proxy, 'https': proxy}
                # headers['User-Agent'] = ua.random
                utils.printf('change proxy2')
                utils.printf('暂停5S')
                time.sleep(5)
                continue  
            with open(fname, mode='w', encoding='utf8') as f:
                f.write(text)
            utils.printf('下载 %s 成功' % fname)
            self.senddistributefinish('process_detail', (article_id, True))
            utils.printf('暂停5S')
            time.sleep(5)


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
            conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
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
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,journal_name,issn,cnno from journal')
        rows = cur.fetchall()
        for journal_id, journal_name, issn, cnno in rows:
            self.dic[journal_id] = (journal_name, issn, cnno)
        cur.close()
        conn.close()
        self.predb3()
        self.sqlList.clear()
        stmt = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, volume, issue, page, beginpage,
        endpage, publisher, subject, date,creator_institution, date_created, source, identifier_pissn, is_oa,
        identifier_cnno, description, identifier_doi, language, country, provider, provider_url, provider_id, type, medium,
        batch, gch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        count = 0
        for filename, fullname in utils.file_list(self.detail_path):
            onemessage = self.parse_detail_one(filename, fullname, 'zt')
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
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
        cur = conn.cursor()
        cur.execute('select journal_id,journal_name,issn,cnno from journal')
        rows = cur.fetchall()
        for journal_id, journal_name, issn, cnno in rows:
            self.dic[journal_id] = (journal_name, issn, cnno)
        cur.close()
        conn.close()
        self.predb3('base_obj_meta_a_template_qk.db3','base_obj_meta_a_qk.hepengineering')
        self.sqlList.clear()
        stmt = """insert into base_obj_meta_a (author,author_1st,organ,organ_1st,title,keyword,pub_year,recv_date,
        accept_date,revision_date,pub_date,vol,num,journal_raw_id,journal_name,page_info,begin_page,end_page,
        is_oa,cited_cnt,down_cnt,lngid,rawid,product,sub_db,provider,sub_db_id,source_type,provider_url,country,
        language,batch,down_date,publisher,issn,cnno,abstract,doi,fulltext_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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
        try:
            language = 'EN'
            country = 'CN'
            provider = 'hepengineeringjournal'
            type_ = 3
            medium = 2
            batch = time.strftime('%Y%m%d') + '00'
            identifier_doi = ''
            rawid = ''
            gch = fullname.split('\\')[-2]
            source, identifier_pissn, identifier_cnno = self.dic[gch]
            publisher = 'Higher Education Press'
            with open(fullname, encoding='utf8') as f:
                text = f.read()
            dicitem = json.loads(text,encoding='utf-8')['resultValue']
            
            
            date_created = dicitem['publicTime']
            if date_created == '':
                date_created = dicitem['onlineTime']
            if date_created != '':
                date_created = replacedate(date_created)
            elif dicitem['year'] != '':
                date_created = str(dicitem['year']) + '0000'
            else:
                date_created = '19000000'
            date = date_created[:4]
            identifier_doi = dicitem['doiNm']
            rawid = filename.replace('.json','')
            
            is_oa = dicitem['isOa']

            # if identifier_doi == '':
            #     articlenum_dic = json.loads(dicitem['attachment'],encoding='utf-8')
            #     if articlenum_dic.__contains__('fileName'):
            #         articlenum = articlenum_dic['fileName'].strip('.pdf')
            #     else:
            #         articlenum = articlenum_dic['key'].split('_')[-1].strip('.pdf')                      
            #     url = 'http://www.engineering.org.cn/en/article/%s' % articlenum
            # else:
            #     url = 'http://www.engineering.org.cn/en/%s' % identifier_doi
            url = 'http://www.engineering.org.cn/default/page/loadPageIndex?pageId=ab4265bb601844d298ec9cd21f046661&id=%s' % rawid.split('_')[-1]
            provider_url = provider + '@' + url
            provider_id = provider + '@' + rawid
            gch = provider + "@" + gch
            lngid = utils.GetLngid('00036', rawid)

            beginpage = str(dicitem['pageStart'])
            endpage = str(dicitem['pageEnd'])           
            if endpage == '' or endpage == '0':
                endpage = beginpage
            page = ''
            if not beginpage == '':
                page = beginpage + '-' + endpage

            volume = dicitem['volumeNm']
            issue = dicitem['issueNm']

            dr = re.compile(r'<[^>]+>',re.S)

            subject = dicitem['keyWords'].replace(',',';').replace('；',';').strip()
            subject = re.sub(r';\s+',';',subject)
            subject = re.sub(r'\s+;',';',subject)
            subject = dr.sub('',subject)

            title = dicitem['title']
            title = dr.sub('',title)                      

            description = dicitem['summary']
            description = dr.sub('',description)
            if description == '' or description == '&nbsp;':
                description = dicitem['content']
                soup = BeautifulSoup(description, 'lxml')
                description = soup.get_text()
            description = description.strip()

            author_1st = ''
            creator = ''
            # authortext = dicitem['articleAuthor'].replace('&eacute;','é').replace('&egrave;','è').replace('&rsquo;','\'')
            authortext = dicitem['articleAuthor']
            if authortext.find('<sup>') > 0:
                authortext = authortext.replace('&nbsp;','').replace('</sup>, ','</sup>、')
                for author in authortext.split('、'):
                    author = author.strip()
                    # utils.printf(author)
                    ptsup = re.compile('.*?(<sup>(.*)</sup>).*?')                                       
                    m = ptsup.match(author)
                    if m:
                        dauthor = author.replace(m.group(1),'').strip()
                        dauthor = dr.sub('',dauthor)
                        if author_1st == '':
                            author_1st = dauthor                                             
                        creator = creator + '%s[%s];' % (dauthor, dr.sub('',m.group(2).strip().strip(',')))
            else:
                creator = authortext.replace('、&nbsp; ',';').replace('、 ',';').replace('、',';')
                creator = dr.sub('',creator)
                creator = creator.replace('&nbsp;','')
                if author_1st == '':
                    author_1st = creator.split(';')[0]
            creator = creator.strip(';').replace(' and ',';')
            creator = html.unescape(creator)
            
            organ_1st = ''
            creator_institution = ''
            institutiontext = dicitem['authorUnit']
            if creator.find('[') > -1:
                if institutiontext.find('<sup>') > -1:
                    institutiontext = institutiontext.replace('<sup>','<br /><sup>')
                    for ins in institutiontext.split('<br />'):
                        ins = ins.strip()
                        ptsup = re.compile('.*(<sup>(.*?)</sup>).*')
                        m = ptsup.match(ins)
                        if m:
                            dins = ins.replace(m.group(1),'')
                            dins = dr.sub('',dins).strip()
                            if organ_1st == '':
                                organ_1st = dins.strip('. ')
                            creator_institution = creator_institution + '[%s]%s;' % (m.group(2).strip(),dins.strip('. '))
                elif institutiontext.find('<p>') > -1:
                    soup = BeautifulSoup(institutiontext, 'lxml')
                    ptp = re.compile('^(\w)\s?\.\s?(.*?)$')
                    for pTag in soup.select('p'):
                        ptext = pTag.get_text()
                        m = ptp.match(ptext)
                        if m:
                            if organ_1st == '':
                                organ_1st = m.group(2).strip()
                            creator_institution = creator_institution + '[%s]%s;' % (m.group(1).strip(),m.group(2).strip())                   
                else:
                    creator_institution = dr.sub('',institutiontext)
            else:
                
                creator_institution = dr.sub('',institutiontext)  
            creator_institution = creator_institution.replace('&nbsp;','').replace('&#39;','\'').strip(';').replace(';;',';')
            organ_1st = organ_1st.replace('&nbsp;','').replace('&#39;','\'').strip(';').replace(';;',';')
            organ_1st = html.unescape(organ_1st)
            creator_institution = html.unescape(creator_institution)
            creator_institution = creator_institution.replace('(','').replace(')','').replace('（','').replace('）','')

            if db3type == 'zt':           
                onemessage = (
                    lngid, rawid, creator, title, volume, issue, page, beginpage, endpage, publisher, subject, date,
                    creator_institution, date_created, source, identifier_pissn, is_oa, identifier_cnno,
                    description, identifier_doi, language, country, provider, provider_url, provider_id, type_, medium,
                    batch, gch
                )
                return onemessage

            recv_date = dicitem['receiveTime']
            if recv_date != '':
                recv_date = replacedate(recv_date)
            accept_date = dicitem['onlineTime']
            if accept_date != '':
                accept_date = replacedate(accept_date)
            revision_date = dicitem['backTime']
            if revision_date != '':
                revision_date = replacedate(revision_date)
            journal_raw_id = fullname.split('\\')[-2]
            
            cited_cnt = dicitem['citedCount']
            if cited_cnt:
                cited_cnt = str(cited_cnt)
            else:
                cited_cnt = 0
            down_cnt = dicitem['downloadCount']
            if down_cnt:
                down_cnt = str(down_cnt)
            else:
                down_cnt = 0

            sub_db = 'QK'
            product = 'ENGINEERING'
            provider = 'HEP'
            sub_db_id = '00036'
            batch = time.strftime("%Y%m%d_%H%M%S")
            down_date = time.strftime("%Y%m%d")
            if dicitem['ossKey']:
                fulltext_type = 'pdf'
            else:
                fulltext_type = ''

            onemessage = (creator,author_1st,creator_institution,organ_1st,title,subject,date,recv_date,
                accept_date,revision_date,date_created,volume,issue,journal_raw_id,source,page,beginpage,endpage,
                is_oa,cited_cnt,down_cnt,lngid,rawid,product,sub_db,provider,sub_db_id,type_,url,country,
                language,batch,down_date,publisher,identifier_pissn,identifier_cnno,description,identifier_doi,fulltext_type)
            if db3type == 'meta':
                return onemessage
            else:
                return False
            
            
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            utils.logerror(exMsg)
            utils.logerror(fullname)
            return False

    def gethtml(self, url, feature=None, endwith="</html>"):
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
            if endwith:
                if text.find(endwith) < 0:
                    print('not endwith %s' % endwith)
                    return False
        except:
            exMsg = '* ' + traceback.format_exc()
            print(exMsg)
            return False
        return resp

    def down_cover(self, message):
        utils.printf('开始下载图片')
        if not self.cover_path:
            self.initpath()
        self.refreshproxypool()
        conn = utils.init_db('mysql', 'hepengineeringjournal', 4)
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
        provider = 'hepengineeringjournal'
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


oneHepEngineeringJournal = HepEngineeringJournal('hepengineeringjournal', 'proxy_cnki')

if __name__ == '__main__':
    oneHepEngineeringJournal.startmission()