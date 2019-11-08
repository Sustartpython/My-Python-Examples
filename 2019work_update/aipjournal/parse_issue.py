import os
import sys
import time
import re
from bs4 import BeautifulSoup
import utils
import traceback
import time


def parse_issue():
    utils.printf('解析列表页开始...')
    # super().parse_index()
    conn = utils.init_db('mysql', 'aipjournal')
    cur = conn.cursor()
    result = []
    stmt = 'insert ignore into article(url,vol,issue,catalog) Values(%s,%s,%s,%s)'
    path = r'E:\lqx\AIP\issue'
    cnt = 0
    for filename, fullname in utils.file_list(path):
        with open(fullname, encoding='utf8') as f:
            text = f.read()
        soup = BeautifulSoup(text, 'lxml')
        urllist = soup.find_all('a', class_='ref nowrap')
        vol = filename.split('_')[0]
        issue = filename.split('_')[-1].split('.')[0]
        print(fullname)
        catalog = fullname.split('\\')[-2]
        base_url = 'https://aip.scitation.org'
        if catalog == 'pto':
            base_url = 'https://physicstoday.scitation.org'
        elif catalog == 'sdy':
            base_url = 'https://aca.scitation.org'
        for x in urllist:
            doiurl = x.get('href').split('/')
            url = base_url + '/doi/abs/' + doiurl[-2] + '/' + doiurl[-1]
            result.append((url, vol, issue, catalog))
        if utils.parse_results_to_sql(conn, stmt, result, 1000):
            cnt += len(result)
            result.clear()
            utils.printf(cnt)
    utils.parse_results_to_sql(conn, stmt, result)
    cnt += len(result)
    utils.printf(cnt)
    conn.close()
    utils.printf('解析列表页完成...')


if __name__ == '__main__':
    parse_issue()