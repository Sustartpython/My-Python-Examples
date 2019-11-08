import requests
import time
import os
import pypyodbc
from parsel import Selector

html_path = r'E:\work\post_card\yb21\html'
dbpath = r'E:\work\post_card\yb21\yb21_postcard_20191107.mdb'

def down_html():
    url = 'http://www.yb21.cn'
    res = requests.get(url)
    if res.status_code == 200:
        res.encoding = res.apparent_encoding
        html = Selector(res.text,'html')
        name = html.xpath("//div[@class='citysearch']/h1/text()").extract()
        for i,item in enumerate(name):
            sheng = item
            link_xpath = "//div[@class='citysearch'][%s]/ul/a/@href" % (i+1)
            link_name_xpath = "//div[@class='citysearch'][%s]/ul/a//text()" % (i+1)
            links = html.xpath(link_xpath).extract()
            link_name = html.xpath(link_name_xpath).extract()
            for n,ii in enumerate(links):
                shi = link_name[n]
                link = url + ii
                shi = shi.replace("邮政编码",'')
                if shi == '辖区':
                    shi = sheng
                if shi == '辖县':
                    shi = sheng
                if shi == '辖市':
                    shi = sheng
                res_2 = requests.get(link)
                if res_2.status_code == 200:
                    res_2.encoding = res_2.apparent_encoding
                    html_2 = Selector(res_2.text,'html')
                    link_3 = html_2.xpath("//table[2]//td/a/@href").extract()
                    for nu,link_ in enumerate(link_3):
                        name = html_2.xpath("//table[2]//td/a/text()").extract()[nu]
                        real_link = url + link_
                        fpath = html_path + '/' + sheng + "/" + shi + '/' + name
                        if not os.path.exists(fpath):
                            os.makedirs(fpath)
                        fname = fpath + '/' + '1.html'
                        if os.path.exists(fname):
                            print('%s 存在' % fname)
                            continue
                        res_4 = requests.get(real_link)
                        if res_4.status_code == 200:
                            fname = fpath + '/' + '1.html'
                            try:
                                with open(fname,'w',encoding='gb18030')as f:
                                    f.write(res_4.content.decode('gb18030'))
                            except:
                                with open(fname,'w',encoding='GB2312')as f:
                                    f.write(res_4.content.decode('GB2312'))
                            print('write %s right' % fname)

def parsel_html():
    db = pypyodbc.win_connect_mdb(dbpath)
    for roots, dirs, files in os.walk(html_path):
        for file in files:
            fpath = roots + '/' + file
            all_ = fpath.replace("E:\work\post_card\yb21\html\\",'').replace("/1.html",'')
            all_list = all_.split('\\')
            sheng = all_list[0]
            shi = all_list[1]
            qu = all_list[2]
            try:
                with open(fpath,encoding='gb18030')as f:
                    text = f.read()
            except:
                with open(fpath,encoding='GB2312')as f:
                    text = f.read()
            html = Selector(text,'html')
            post = ""
            post_list = html.xpath("//tr/td/strong/a/text()").extract()
            for item in post_list:
                post += item + ';'
            sql = "insert into post_card(省,市,区县,邮编) values('%s','%s','%s','%s')" % (sheng, shi, qu, post)
            curser = db.cursor()
            curser.execute(sql)
            curser.commit()
            print('%s插入成功' % qu)


if __name__ == "__main__":
    # down_html()
    parsel_html()