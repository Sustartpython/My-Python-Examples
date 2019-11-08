import requests
import pypyodbc
import time
import re
from parsel import Selector
from bs4 import BeautifulSoup

dbpath = 'E:\work\post_card\mca\mca_post_20191106.mdb'

def down_html():
    url = 'http://www.mca.gov.cn/article/sj/tjyb/qgsj/2019/201909291543.html'
    res = requests.get(url)
    res.encoding = 'utf-8'
    if res.status_code == 200:
        fee = 'xl7126399'
        if res.text.find(fee) > 0:
            with open('post.html','w',encoding='utf-8')as f:
                f.write(res.text)
            print('下载成功')
        else:
            print('not find fee')
    else:
        print('status_code not 200')

def parse_html():
    db = pypyodbc.win_connect_mdb(dbpath)
    dict_max = dict()
    dict_mid = dict()
    with open('post.html',encoding='utf-8')as f:
        text = f.read()
    html = Selector(text,'html')
    tr = html.xpath("//tr[@height='19']")
    for item in tr.xpath('string(.)').extract():
        item = item.strip().replace('\n','')
        card = re.findall('[0-9]{6}',item)[0]
        address = re.findall(r'[\u4e00-\u9fa5].*',item)[0]
        geshu = item.replace(card,'').replace(address,'')
        # # 空位2个为最大级别
        if len(geshu) == 2:
            dict_max[card[0:2]] = address
            sheng = address
            shi = ' '
            qu = ' '
            all_ = card + '-' + sheng + '-' + shi + '-' + qu
        # # 空位3个为第二
        if len(geshu) == 3:
            dict_mid[card[0:4]] = address
            max_ = card[0:2]
            sheng = dict_max[max_]
            shi = address
            qu = ' '
            all_ = card + '-' + sheng + '-' + shi + '-' + qu
        if len(geshu) == 5:
            max_ = card[0:2]
            sheng = dict_max[max_]
            mid_ = card[0:4]
            try:
                shi = dict_mid[mid_]
            except:
                shi = sheng
            qu = address
            all_ = card + '-' + sheng + '-' + shi + '-' + qu
        # print(all_)
        all_list = all_.split('-')
        card = all_list[0]
        sheng = all_list[1].replace(' ','')
        shi = all_list[2].replace(' ','')
        qu = all_list[3]
        sql = "insert into post_card(省,市,区县,代码) values('%s','%s','%s','%s')" % (sheng, shi, qu, card)
        curser = db.cursor()
        curser.execute(sql)
        curser.commit()
        print('%s插入成功' % card)
    


if __name__ == "__main__":
    # down_html()
    parse_html()