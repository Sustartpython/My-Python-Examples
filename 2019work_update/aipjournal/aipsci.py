import os
import sys
import time
import re
import requests
import random
from bs4 import BeautifulSoup
import utils
import traceback
import time

Headers = {
    'Accept':
        '*/*',
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
}

cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))


def _downlist(url):
    index_dir = cur_dir_fullpath + '/issue/sci'
    index_file = index_dir + '/' + url[-1] + '.html'
    if os.path.exists(index_file):
        return True
    feature = 'Publication Date'
    try:
        resp = utils.get_html(url, feature=feature)
    except:
        # exMsg = '* ' + traceback.format_exc()
        # print(exMsg)
        return -1
    if not resp:
        return -1
    if resp.text.find(feature) < 0:
        return -1
    if resp.text.find('</html>') < 0:
        return -1

    if not os.path.exists(index_dir):
        os.makedirs(index_dir)

    with open(index_file, mode='w', encoding='utf8') as f:
        f.write(resp.content.decode('utf8'))
    utils.printf('下载', url, '成功...')
    return True


def downlist():
    for page in range(7):
        url = 'https://aip.scitation.org/toc/sci/current?pageSize=100&startPage={}'
        url = url.format(str(page))
        _downlist(url)


def parselist():
    utils.printf('解析列表页开始...')
    # super().parse_index()
    conn = utils.init_db('mysql', 'aipjournal')
    cur = conn.cursor()
    result = []
    stmt = 'insert ignore into sciarticle(url,catalog) Values(%s,%s)'
    path = cur_dir_fullpath + '/issue/sci'
    cnt = 0
    for filename, fullname in utils.file_list(path):
        with open(fullname, encoding='utf8') as f:
            text = f.read()
        soup = BeautifulSoup(text, 'lxml')
        urllist = soup.find_all('a', class_='ref nowrap')
        print(fullname)
        catalog = 'sci'
        for x in urllist:
            doiurl = x.get('href').split('/')
            url = 'https://aip.scitation.org/doi/abs/' + doiurl[-2] + '/' + doiurl[-1]
            result.append((url, catalog))
        if utils.parse_results_to_sql(conn, stmt, result, 100):
            cnt += len(result)
            result.clear()
            utils.printf(cnt)
    utils.parse_results_to_sql(conn, stmt, result)
    cnt += len(result)
    utils.printf(cnt)
    conn.close()
    utils.printf('解析列表页完成...')


def downdetail():
    conn = utils.init_db('mysql', 'aipjournal')
    cur = conn.cursor()
    sql = 'select url,catalog from sciarticle where stat=0 and failcount<3'
    cur.execute(sql)
    rows = cur.fetchall()
    detail_dir = cur_dir_fullpath + '/article/sci'
    for url, catalog in rows:
        urllist = url.split('/')
        detail_file = detail_dir + '/' + urllist[-2] + '_' + urllist[-1] + '.html'
        flag = downdetailone(url, detail_dir, detail_file)
        if flag:
            stmt = "update sciarticle set stat=1 where url='{}'".format(url)
        else:
            stmt = "update sciarticle set failcount=failcount+1 where url='{}'".format(url)
        # print(stmt)
        cur.execute(stmt)
        conn.commit()
        time.sleep(3)
    utils.printf('下载详情页完成...')


def downdetailone(url, detail_dir, detail_file):
    if os.path.exists(detail_file):
        return True
    feature = 'publicationContentTitle'
    try:
        resp = utils.get_html(url, feature=feature)
    except:
        # exMsg = '* ' + traceback.format_exc()
        # print(exMsg)
        return False
    if not resp:
        return False
    if resp.text.find('</html>') < 0:
        return False

    if not os.path.exists(detail_dir):
        os.makedirs(detail_dir)

    with open(detail_file, mode='w', encoding='utf8') as f:
        f.write(resp.content.decode('utf8'))
    utils.printf('下载', url, '成功...')
    return True


def parse_detail():
    utils.printf('解析详情页开始...')
    language = 'EN'
    provider = 'aipjournal'
    type_ = 3
    medium = 2
    batch = time.strftime('%Y%m%d') + '00'
    sql = """insert or ignore into modify_title_info_zt(lngid, rawid, creator, title, date, date_created, identifier_eissn, source, description,identifier_doi, language, country, provider, provider_url, provider_id, type, medium, batch, gch)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    detail_dir = cur_dir_fullpath + '/article/sci'
    template_file = cur_dir_fullpath + '/product/aip_sci.db3'

    conn = utils.init_db("sqlite3", template_file)

    result = []
    for filename, filepath in utils.file_list(detail_dir):
        [title, creator, date_created, description] = _parse_detail_one(filepath)
        identifier_doi = filename.replace('.html', '').replace('_', '/')
        rawid = identifier_doi
        lngid = 'AIP_WK_' + rawid
        provider_url = provider + '@' + 'https://aip.scitation.org/doi/abs/' + identifier_doi
        provider_id = provider + '@' + identifier_doi
        country = 'US'
        date = date_created[0:4]
        source = 'Scilight'
        identifier_eissn = '2572-7907'
        gch = provider + '@' + 'sci'

        result.append(
            (
                lngid, rawid, creator, title, date, date_created, identifier_eissn, source, description,
                identifier_doi, language, country, provider, provider_url, provider_id, type_, medium, batch, gch
            )
        )

        if utils.parse_results_to_sql(conn, sql, result, 100):
            result.clear()
        utils.printf(lngid)
    # print(result)
    utils.parse_results_to_sql(conn, sql, result)
    conn.close()


def _parse_detail_one(filepath):
    with open(filepath, encoding='utf8') as fp:
        text = fp.read()
    soup_one = BeautifulSoup(text, 'lxml')

    titleTag = soup_one.select_one("div.articleMeta > header > h3")
    title = ''
    if titleTag:
        title = ''.join(titleTag.stripped_strings)

    authorTags = soup_one.select("div.articleMeta > div.entryAuthor > a")
    author = ''
    for authorTag in authorTags:
        author = author + ''.join(authorTag.stripped_strings) + ';'
    author = author.strip(";")

    date_created = ''
    dateTag = soup_one.select_one("div.clear.date-download-container > div > span.date")
    if dateTag:
        date = ''.join(dateTag.stripped_strings)
        if len(date) > 4:
            date_created = replacedate(date).strip()

    descriptionTag = soup_one.select_one("div.articleMeta > div.teaser > div")
    description = ''
    if descriptionTag:
        description = ''.join(descriptionTag.stripped_strings)
    # print(title)
    # print(author)
    # print(pisbn)
    # print(eisbn)
    # print(date_created)
    # print(price)
    # print(imgurl)
    # print(cyright)
    # print(description)
    data = (title, author, date_created, description)
    return data


def replacedate(date):
    datelist = date.split(' ')
    mapmonth = {
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
    return datelist[2] + mapmonth[datelist[1]] + datelist[0]


if __name__ == '__main__':
    downlist()
    parselist()
    downdetail()
    parse_detail()
