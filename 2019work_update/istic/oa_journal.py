"""
中信所OA期刊
数量:753
"""
import utils
import json
import time
import pypyodbc
from parsel import Selector

listpath = r'E:\work\istic\list'
detailpath = r'E:\work\istic\detail'
dbpath = r'E:\work\istic\istic_oa_20191113.mdb'

db = pypyodbc.win_connect_mdb(dbpath)

def down_index():
    url = "http://doaj.istic.ac.cn/api/Service/Istic/SearchIstic/Post"
    data  = {
        'pxStr': 'XG',
        'pageCount': '1000',
        'pageIndex': '0',
        'searchtype': 'JN',
        'searchvalue': '',
        'xk': 'ALL',
        'dq': 'ALL',
        'zq': 'ALL',
    }
    res = utils.get_html_by_post(url,data=data)
    fname = listpath + '/1.html'
    with open(fname,'w',encoding='utf8')as f:
        f.write(res.text)
    utils.printf("下载post页面成功")

def parse_index():
    fname = listpath + '/1.html'
    with open(fname,encoding='utf-8')as f:
        text = f.read()
    text = text[1:-1].replace("\\\"","\"")
    PerList = json.loads(text)['PerList']
    for item in PerList:
        j_id = item["PerId"]
        j_name = item['PerName']
        sql = "insert into journal (期刊id,期刊名称) values ('%s','%s')" % (j_id,j_name)
        curser = db.cursor()
        curser.execute(sql)
        curser.commit()
        utils.printf("第一次插入%s成功" % j_name)

def down_detail():
    sql = "select 期刊id,期刊名称 from journal"
    curser = db.cursor()
    curser.execute(sql)
    row = curser.fetchall()
    if len(row) == 0:
        utils.printf("无数据")
    else:
        for j_id,journal_name in row:
            url = "http://doaj.istic.ac.cn/istic/Periodical/Info?id=%s" % j_id
            feature = "perBody"
            resp = utils.get_html(url,feature=feature)
            if not resp:
                continue
            else:
                fname = '%s/%s.html' %(detailpath,j_id)
                with open(fname,'w',encoding='utf8')as f:
                    f.write(resp.content.decode('utf8'))
                utils.printf("下载%s成功" % fname)

def parse_detal():
    for file, fullpath in utils.file_list(detailpath):
        j_id = file.replace(".html",'')
        with open(fullpath,encoding='utf8')as f:
            text = f.read()
        html = Selector(text,'html')
        title = html.xpath("//h3/text()").extract_first("")
        title_en = html.xpath("//h4/text()").extract_first("").replace("'","''")
        div = html.xpath("//div[@class='perinfo']/text()").extract()
        zbdw = dq = issn = cn = shijian = ""
        for item in div:
            if item.startswith("主办单位："):
                zbdw = item.replace("主办单位：","")
            if item.startswith("地区："):
                dq = item.replace("地区：","")
            if item.startswith("国际刊号："):
                issn = item.replace("国际刊号：","")
            if item.startswith("国内刊号："):
                cn = item.replace('国内刊号：','')
            if item.startswith("出版周期："):
                shijian = item.replace("出版周期：","")
        # utils.printf(title,title_en,zbdw,dq,issn,cn,shijian)
        sql = "update journal set 期刊名称_外文 = '%s' , 主办单位 = '%s' , 地区 = '%s' , 国际刊号 = '%s' , 国内刊号 = '%s' , 出版周期 = '%s' where 期刊id = '%s'" % (title_en,zbdw,dq,issn,cn,shijian,j_id)
        curser = db.cursor()
        curser.execute(sql)
        curser.commit()
        utils.printf("更新%s信息成功" % title)

if __name__ == "__main__":
    # down_index()
    # parse_index()
    # down_detail()
    parse_detal()
    