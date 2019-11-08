import os
import re
import json
import time
import toml
import base64
import hashlib
import pymysql
import requests
from PIL import Image
from parsel import Selector

class swjtu_book_spider(object):
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        self.proxy = {
            'http':'192.168.30.176:8131'
        }
        #读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        #链接数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        # 读取配置文件获取存储路径
        self.listPath = toml.load('config.toml')['listPath']
        self.detailPath = toml.load('config.toml')['detailPath']
        self.imagelPath = toml.load('config.toml')['imagePath']
        if not os.path.exists(self.listPath):
            os.makedirs(self.listPath)
        if not os.path.exists(self.detailPath):
            os.makedirs(self.detailPath)
        if not os.path.exists(self.imagelPath):
            os.makedirs(self.imagelPath)

    def BaseEncodeID(self,strRaw):
        r""" 自定义base编码 """

        strEncode = base64.b32encode(strRaw.encode('utf8')).decode('utf8')

        if strEncode.endswith('======'):
            strEncode = '%s%s' % (strEncode[0:-6], '0')
        elif strEncode.endswith('===='):
            strEncode = '%s%s' % (strEncode[0:-4], '1')
        elif strEncode.endswith('==='):
            strEncode = '%s%s' % (strEncode[0:-3], '8')
        elif strEncode.endswith('='):
            strEncode = '%s%s' % (strEncode[0:-1], '9')

        table = str.maketrans('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210')
        strEncode = strEncode.translate(table)

        return strEncode
    
    def BaseDecodeID(self,strEncode):
        r""" 自定义base解码 """

        table = str.maketrans('ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210', '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        strEncode = strEncode.translate(table)

        if strEncode.endswith('0'):
            strEncode = '%s%s' % (strEncode[0:-1], '======')
        elif strEncode.endswith('1'):
            strEncode = '%s%s' % (strEncode[0:-1], '====')
        elif strEncode.endswith('8'):
            strEncode = '%s%s' % (strEncode[0:-1], '===')
        elif strEncode.endswith('9'):
            strEncode = '%s%s' % (strEncode[0:-1], '=')

        strRaw = base64.b32decode(strEncode.encode('utf8')).decode('utf8')

        return strRaw

    def GetLngid(self,sub_db_id, rawid, case_insensitive=False):
        uppercase_rawid = ''  # 大写版 rawid
        if case_insensitive:  # 源网站的 rawid 区分大小写
            for ch in rawid:
                if ch.upper() == ch:
                    uppercase_rawid += ch
                else:
                    uppercase_rawid += ch.upper() + '_'
        else:
            uppercase_rawid = rawid.upper()

        limited_id = uppercase_rawid  # 限长ID
        if len(uppercase_rawid) > 20:
            limited_id = hashlib.md5(uppercase_rawid.encode('utf8')).hexdigest().upper()
        else:
            limited_id = self.BaseEncodeID(uppercase_rawid)

        lngid = sub_db_id + limited_id

        return lngid



    def read_sql(self,sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        sql_data = cur.fetchall()
        return sql_data

    def down_index(self):
        for page_num in range(1,251):
            path = self.listPath + '/%s.html' % str(page_num)
            # if os.path.exists(path):
            #     print(path + "存在")
            #     self.insert_index(path)
            #     continue
            index_url = "http://mirror.lib.swjtu.edu.cn:90/?app=organization&controller=index&action=orgebook_list&page=%s&pagesize=12&show=img&order=&sort=" % str(page_num)
            try:
                res = requests.get(index_url, headers=self.headers, proxies=self.proxy)
                if res.status_code == 200:
                    with open(path, mode='w', encoding='utf-8') as f:
                        f.write(res.content.decode('utf8'))
                    print('下载第%s页成功' % str(page_num))
                    self.insert_index(path)
                else:
                    print('下载第%s页出现ip错误' % str(page_num))
                    time.sleep(999)
            except:
                continue
        
    def insert_index(self,path):
        with open(path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        urls = html.xpath("//a[@class='main_lib_book_a']/@href").getall()
        for i, item in enumerate(urls):
            link = "http://mirror.lib.swjtu.edu.cn:90/" + item
            image_url = html.xpath("//a[@class='main_lib_book_a']/img/@src").getall()[i]
            title =  html.xpath("//a[@class='main_lib_book_a']/img/@alt").getall()[i]
            sql = "insert ignore into list(title,url,image_url) values ('%s','%s','%s')" % (title,link,image_url)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
    
    def down_detail(self):
        sql = "select url,image_url from list where stat = 0"
        datas = self.read_sql(sql)
        if datas == ():
            return
        else:
            for data in datas:
                detail_url = data[0]
                image_url = data[1]
                bookid = re.findall('bookid=(.*)',detail_url)[0]
                catalog_url = "http://mirror.lib.swjtu.edu.cn:90/?app=organization&controller=import&action=unpack&bookid=%s" % bookid
                try:
                    detailname = self.detailPath + '/%s.html' % bookid
                    res1 = requests.get(detail_url,headers=self.headers,proxies=self.proxy)
                    if res1.status_code == 200:
                        with open(detailname, mode='w', encoding='utf-8') as f:
                            f.write(res1.content.decode('utf8'))
                            print('下载第%s本成功' % bookid)
                except:
                    print('下载第%s本错误' % bookid)
                    time.sleep(999)
                try:
                    imagename = self.imagelPath + '/%s.jpg' % bookid
                    res2 = requests.get(image_url,headers=self.headers,proxies=self.proxy)
                    if res1.status_code == 200:
                        with open(imagename, mode='wb') as f:
                            f.write(res2.content)
                        srcImg = Image.open(imagename)
                        dstImg = srcImg.resize((108, 150), Image.ANTIALIAS).convert('RGB')
                        dstImg.save(imagename)
                        print('下载第%s本封面成功' % bookid)
                except:
                    print('下载第%s本封面错误' % bookid)
                    time.sleep(999)
                message = [detail_url,catalog_url,detailname,bookid]
                self.paser_datail(message)
    
    def paser_datail(self,message):
        detail_url = message[0]
        catalog_url = message[1]
        detailname = message[2]
        bookid = message[3]
        rawid = bookid
        sub_db_id = "00171"
        provider = "mirrorswjtucrphdmbook"
        provider_url = provider+"@"+ detail_url
        provider_id = provider+"@"+rawid
        type_ = "1"
        medium = "2"
        lngid = self.GetLngid(sub_db_id, rawid, case_insensitive=False)
        batch = time.strftime('%Y%m%d',time.localtime(time.time()))
        batch = batch + '00'
        language = "ZH"
        country = "CN"
        cover = '/smartlib/mirrorswjtucrphdmbook/'+ rawid +'.jpg'
        #解析详情页
        with open(detailname, encoding='utf8') as f:
            text_one = f.read()
        detailhtml = Selector(text_one, type='html')
        #来源专辑/分类
        provider_subject = ""
        provider_subjects = detailhtml.xpath("//title/text()").extract_first("").split(" - ")[1:3]
        for i,item in enumerate(provider_subjects):
            provider_subject += item + ';'
            provider_subject = provider_subject.replace("电子图书;","")
        #题名
        title = detailhtml.xpath("//h2[@class='book_title']/text()").extract_first("")
        #作者
        creator = detailhtml.xpath("//li[@class='book_detail_author']/a/text()").extract_first("").replace(",",";")
        #出版日期
        date_created = detailhtml.xpath("//li[@class='book_detail_time']/text()").extract_first("19000000").replace("出版日期：","").replace(".","") + "00"
        #出版年
        date = date_created[:4]
        #出版社
        publisher = detailhtml.xpath("//li[@class='book_detail_press']/text()").extract_first("").replace("出版社：","")
        #ISBN（数字）
        identifier_eisbn = detailhtml.xpath("//li[@class='book_detail_isbn']/text()").extract_first("").replace("ISBN：","").replace("-","")
        #摘要
        description = detailhtml.xpath("//div[@class='desc']/text()").extract_first("").replace("'","\"")
        #插入detail表
        sql_1 = "insert ignore into detail(title,creator,url,date_create,publisher,identifier_eisbn,provider_subject,catalog_url,description) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (title,creator,detail_url,date_created,publisher,identifier_eisbn,provider_subject,catalog_url,description)
        cur_1 = self.conn.cursor()
        cur_1.execute(sql_1)
        self.conn.commit()
        #访问目录页ajax请求
        res = requests.get(catalog_url,headers=self.headers,proxies=self.proxy)
        text = res.text
        data = json.loads(text)
        description_unit = ""
        for i,item in enumerate(data['data']):
            titlex = item['title']
            try:
                nodes = item['node']
                jie = ""
                for n,node in enumerate(nodes):
                    jie += node['title'] + ";"
                    #目录
                description_unit += titlex + ":" + jie + "\n"
                description_unit = description_unit.replace("'","\"")
            except:
                description_unit += titlex + '\n'
                description_unit = description_unit.replace("'","\"")
        #插入智图表
        sql_2 = "insert ignore into modify_title_info_zt(lngid,rawid,title,identifier_eisbn,creator,publisher,date,description,description_unit,date_created,cover,language,country,type,provider,provider_url,provider_id,provider_subject,medium,batch) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(lngid,rawid,title,identifier_eisbn,creator,publisher,date,description,description_unit,date_created,cover,language,country,type_,provider,provider_url,provider_id,provider_subject,medium,batch)
        cur_2 = self.conn.cursor()
        cur_2.execute(sql_2)
        self.conn.commit()
        sql_3 = "update list set stat=1 where url = '%s'" % (detail_url)
        cur_3 = self.conn.cursor()
        cur_3.execute(sql_3)

           

if __name__ == "__main__":
    book = swjtu_book_spider()
    book.down_index()
    print("下载列表页结束,开始下载详情页")
    book.down_detail()


    




