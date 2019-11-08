import requests
import time
import os
import pypyodbc
from parsel import Selector

html_path = r'E:\work\post_card\ip138\html'
dbpath = r'E:\work\post_card\ip138\ip138_postcard_20191107.mdb'
def down_html():
    url = 'http://alexa.ip138.com/post/'
    res = requests.get(url)
    if res.status_code == 200:
        res.encoding = res.apparent_encoding
        html = Selector(res.text,'html')
        link = html.xpath("//div[@id='newAlexa']//tr/td/a/@href").extract()
        names = html.xpath("//div[@id='newAlexa']//tr/td/a/text()").extract()
        for i,item in enumerate(link):
            url_2 = 'http://alexa.ip138.com' + item
            name = names[i]
            res_2 = requests.get(url_2)
            # res_2.encoding = res_2.apparent_encoding
            fname = html_path + '/' + name + '.html'
            with open(fname,'w',encoding='gb18030')as f:
                f.write(res_2.content.decode('gb18030'))
            print('write %s right' % name)

def parse_html():
    result = []
    result_shi = []
    db = pypyodbc.win_connect_mdb(dbpath)
    for root, dirs, files in os.walk(html_path):
        for file in files:
            dict_mid = dict()
            # 最大一级
            sheng = file.replace('.html','')
            print(sheng)
            fname = root + '/' + file
            with open(fname,encoding='gb18030')as f:
                text = f.read()
            html = Selector(text)
            tr = html.xpath("//table[@class='t12']//tr[@bgcolor='#ffffff']")
            for item in tr:
                shi = item.xpath('./td[1]/a/b/text()').extract_first('')
                if shi == '辖区':
                    shi = sheng
                if shi == '辖县':
                    shi = sheng
                if shi == '':
                    qu_1 = item.xpath('./td[1]/a/text()').extract_first('').replace('\xa0','')
                    qu_1_post = item.xpath('./td[2]/text()').extract_first('').replace('\xa0','')
                    if qu_1 == qu_1_post == '':
                        print('空白')
                    else:
                        all_1 = sheng + '-' + result_shi[0] + '-' +  qu_1 + '-' + qu_1_post
                        result.append(all_1)
                    qu_2 = item.xpath("./td[4]/a/text()").extract_first('').replace('\xa0','')
                    qu_2_post = item.xpath('./td[5]/text()').extract_first('').replace('\xa0','')
                    if qu_2 == qu_2_post == '':
                        print('空白')
                    else:
                        all_2 = sheng + '-' + result_shi[0] + '-' +  qu_2 + '-' + qu_2_post
                        result.append(all_2)
                else:
                    result_shi.clear()
                    result_shi.append(shi)
                    shi_post = item.xpath('./td[2]/text()').extract_first('').replace('\xa0','')
                    all_3 = sheng + '-' + shi + '-' +" "+ '-' + shi_post
                    result.append(all_3)
        for item in result:
            # print(item)
            all_list = item.split('-')
            sheng = all_list[0].replace(' ','')
            shi = all_list[1].replace(' ','')
            qu = all_list[2].replace(' ','')
            post = all_list[3].replace(' ','')
            sql = "insert into post_card(省,市,区县,邮编) values('%s','%s','%s','%s')" % (sheng, shi, qu, post)
            curser = db.cursor()
            curser.execute(sql)
            curser.commit()
            print('%s插入成功' % qu)

if __name__ == "__main__":
    # down_html()
    parse_html()