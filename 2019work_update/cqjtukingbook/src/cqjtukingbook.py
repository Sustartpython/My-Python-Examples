"""
金图外文--重庆交大采集任务
url：http://123.56.143.23/kingbookwaiwen/book/list.aspx?sw=&Field=&code=&pageindex=12945
站点资源数量：258897
"""
import os
import sys
import re
import time
from bs4 import BeautifulSoup
import traceback
# 将 utils 加入到 PYTHONPATH 中
# pwd = os.path.dirname(os.getcwd())
# utils_path = os.path.abspath(os.path.dirname(pwd) + os.path.sep + ".")
# sys.path.insert(0, utils_path)
import utils


class KingBookDownload(utils.Download):

    proxy = {"http": "http://192.168.30.176:8171"}

    def down_list(self):
        super().down_list()
        base_url = "http://123.56.143.23/kingbookwaiwen/book/list.aspx?sw=&Field=&code=&pageindex={page}"
        feature = '<h4 class="entry-title arial"'  #网页特征码
        for page in range(1, 12993):
            filename = self.list_path + '/{page}.html'.format(page=page)
            if os.path.exists(filename):
                continue
            resp = utils.get_html(base_url.format(page=page), feature=feature, proxies=self.proxy, timeout=50)
            if not resp:
                continue
            with open(filename, mode='w', encoding='utf8') as f:
                f.write(resp.content.decode())
            utils.printf("下载第{page}页完成,总共{pages}。".format(page=page, pages=12992))

    def down_detail(self):
        utils.printf("下载详情页开始...")
        super().down_detail()
        conn = utils.init_db('mysql', 'cqjtu_kingbook')
        cur = conn.cursor()
        while True:
            cur.execute('select bookid,stat from book where stat=0 limit 10000')
            rows = cur.fetchall()
            conn.commit()
            if len(rows) == 0:
                break
            for bookid, _ in rows:
                print(bookid)
                url = 'http://123.56.143.23/kingbookwaiwen/book/info.aspx?id={}'.format(bookid)
                dirname = '%s/%s' % (self.detail_path,bookid[:3])
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                filename = '%s/%s.html' % (dirname,bookid)
                if os.path.exists(filename):
                    sql = 'update book set stat=1 where bookid="{}"'.format(bookid)
                    cur.execute(sql)
                    conn.commit()
                    continue
                resp = utils.get_html(url, proxies=self.proxy)
                if not resp:
                    continue
                with open(filename, mode='w', encoding='utf8') as f:
                    f.write(resp.content.decode())
                sql = 'update book set stat=1 where bookid="{}"'.format(bookid)
                cur.execute(sql)
                conn.commit()
                utils.printf("下载", bookid, "成功...")


class KingBookParse(utils.Parse):

    def parse_list(self):
        super().parse_list()
        conn = utils.init_db('mysql', 'cqjtu_kingbook')
        base_url = "http://123.56.143.23/kingbookwaiwen/book/"
        regex_bookid = re.compile(r"info.aspx\?id=(\d+)")
        stmt = 'insert ignore into book (bookid,stat) values(%s,%s)'
        results = []
        for _, filename in utils.file_list(self.list_path):
            with open(filename, encoding='utf8') as f:
                text = f.read()
            bookidlist = regex_bookid.findall(text)
            for bookid in bookidlist:
                results.append((bookid,0))
            if utils.parse_results_to_sql(conn, stmt, results, 1000):
                total = len(results)
                results.clear()
                print('插入 ', total, ' 个结果到数据库成功')
        utils.parse_results_to_sql(conn, stmt, results)
        print('插入 ', len(results), ' 个结果到数据库成功')

    def parse_detail(self):
        super().parse_detail()
        language = "EN"
        type = "1"
        medium = "2"
        provider = "cqjtukingbook"
        country = "US"
        batch = time.strftime('%Y%m%d') + "00"
        stmt = (
            '''insert into modify_title_info_zt(lngid,rawid,title,creator,description,subject,date,date_created,identifier_pisbn,language,country,provider,provider_url,provider_id,type,medium,batch,publisher) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
        )
        conn = utils.init_db("sqlite3", self.template_file)
        results = []
        cnt=0
        for file, fullpath in utils.file_list(self.detail_path):
            with open(fullpath, encoding='utf8') as fp:
                txt = fp.read()
            try:
                title, creator, publishers, date, identifier_pisbn, subject, description = self._parse_detail_one(txt)
            except:
                exMsg = '* ' + traceback.format_exc()
                logerror(fullpath)
                logerror(exMsg)
                continue
            date_created = date + '0000'
            basename, _, ext = file.partition(".")
            rawid = basename
            provider_url = provider + "@http://123.56.143.23/kingbookwaiwen/book/info.aspx?id=" + rawid
            provider_id = provider + "@" + rawid
            lngID = "CQJTU_KINGBOOK_TS_" + rawid
            results.append(
                (
                    lngID, rawid, title, creator, description, subject, date, date_created, identifier_pisbn, language,
                    country, provider, provider_url, provider_id, type, medium, batch, publishers
                )
            )
            if utils.parse_results_to_sql(conn, stmt, results, 1000):
                cnt+=len(results)
                print('%s:%s' % (time.strftime("%Y/%m/%d %X"),cnt))
                results.clear()

        utils.parse_results_to_sql(conn, stmt, results)
        cnt+=len(results)
        print('%s:%s' % (time.strftime("%Y/%m/%d %X"),cnt))
        conn.close()

    def _parse_detail_one(self, detail_txt):

        soup = BeautifulSoup(detail_txt, "lxml")
        title = soup.select_one("#ctl00_contendBody_lbltitle").string
        title = title if title else ""
        creator = soup.select_one("#ctl00_contendBody_lblauthor").string
        creator = creator.replace(", ", ";") if creator else ""

        publishers = soup.select_one("#ctl00_contendBody_lblpublish").string
        publishers = publishers if publishers else ""

        date = soup.select_one("#ctl00_contendBody_lblpubdate").string
        date = date if date else ""
        if date == '0' or date == '':
            date = '1900'

        identifier_pisbn = soup.select_one("#ctl00_contendBody_lblisbn").string
        identifier_pisbn = identifier_pisbn.strip() if identifier_pisbn else ""
        identifier_pisbn = identifier_pisbn.replace('(Cloth)','').replace('-','')

        subject = soup.select_one("#ctl00_contendBody_lblkeyword").string
        subject = subject.replace(",", ";").replace(". ", ";").strip() if subject else ""

        description = soup.select_one("#ctl00_contendBody_lblintro").string
        description = description if description else ""

        # cite = soup.select_one("#lblcite").string
        # cite = cite if cite else ""
        return title, creator, publishers, date, identifier_pisbn, subject, description






def logerror(line):
    # cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))
    logpath = r'E:\lqx\aiaajournal\download\products'
    # if not os.path.exists(logpath):
    #     os.makedirs(logpath)
    fname = logpath + '/' + time.strftime("%Y%m%d") + '.txt'
    try:
        with open(fname, mode='a', encoding='utf8') as f:
            f.write(line + '\n')
    except Exception as e:
        raise e


if __name__ == "__main__":
    down = KingBookDownload()
    parse = KingBookParse()
    # down.down_list()
    # parse.parse_list()
    down.down_detail()
    parse.parse_detail()
