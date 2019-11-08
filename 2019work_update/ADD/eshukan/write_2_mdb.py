import os
import pypyodbc
import re
import time
from parsel import Selector

path = r'E:\work\ADD\eshukan\detail\20190924'
db = pypyodbc.win_connect_mdb(r'E:\work\ADD\eshukan\data\eshukan.mdb')
filenames = os.listdir(path)
for filename in filenames:
    url = "http://www.eshukan.com/displayj.aspx?jid=%s" % filename
    url = url.replace('.html','')
    file = path + '\\' + filename
    with open(file,encoding='utf-8')as f:
        text = f.read()
    html = Selector(text,'html')
    # 标题
    title = html.xpath("//div[@class='jjianjietitle']/h1/text()").extract_first('')
    title = title.replace('（停刊）','').replace('（Email投稿）','').replace('（Email附打印稿）','').replace('（官网投稿）','').replace('（纸质投稿）','').replace('（Email投稿；官网投稿）','').replace('（Email投稿；纸质投稿）','').replace('（打印稿）','').replace('（Email投稿；纸质投稿；打印稿）','').replace('（Email投稿；打印稿）','').replace('（不接收投稿）','').replace('（Email附纸质稿）','').strip().replace("'","^")
    # 简介
    abstract = ""
    abstracts = html.xpath("//div[@class='jjianjiecon']//text()").extract()
    for i in abstracts:
        abstract += i
    abstract = abstract.replace("...[显示全部]","").replace('\xa0','').replace('\n','').strip().replace("'","^").replace('\U00100000','')
    # 总信息
    zgbm = zbdw = bjb = zb = zr = dz = cz = cn = issn = post_card = price = post_size = ''
    sjcon = html.xpath("//div[@class='sjcon']/p")
    for sj in sjcon.xpath('string(.)').extract():
        info = sj.strip()
        if '主管部门：' in info:
            zgbm = re.findall('主管部门：(.*)',info)[0]
        if '主办单位：' in info:
            zbdw = re.findall('主办单位：(.*)',info)[0]
        if '编辑部：' in info:
            bjb = re.findall('编辑部：(.*)',info)[0]
        if '主编：' in info:
            zb = re.findall('主编：(.*)',info)[0]
        if '主任：' in info:
            zr = re.findall('主任：(.*)',info)[0]
        if '地址：' in info:
            dz = re.findall('地址：(.*)',info)[0].replace("'","^")
        if '传真：' in info:
            cz = re.findall('传真：(.*)',info)[0]
        if '邮编：' in info:
            post_card = re.findall('邮编：(.*)',info)[0]
        if '定价：' in info:
            price = re.findall('定价：(.*)',info)[0]
        if '邮发代码：' in info:
            post_size = re.findall('邮发代码：(.*)',info)[0]
        if '国内统一刊号：' in info:
            cn = re.findall('国内统一刊号：(.*)',info)[0].replace('CN','').strip()
        if '国际标准刊号：' in info:
            issn = re.findall('国际标准刊号：(.*)',info)[0]

    # 征稿信息
    ways = journal_url = email = phone = date = ''
    way = html.xpath("//div[@class='zgcon']/p")
    for p in way.xpath('string(.)').extract():
        if "投稿方式：" in p:
            ways = re.findall('投稿方式：(.*)',p)[0]
        if '刊内网址：' in p:
            journal_url = re.findall('刊内网址：(.*)',p)[0]
        if '刊内邮箱：' in p:
            email = re.findall('刊内邮箱：(.*)',p)[0]
        if '刊内电话：' in p:
            phone = re.findall('刊内电话：(.*)',p)[0]
        if '出刊日期：' in p:
            date = re.findall('出刊日期：(.*)',p)[0]

    sql = "insert into data(url,期刊名称,简介,主管部门,主办单位,编辑部,主编,主任,地址,邮编,电话,传真,刊内邮箱,网址,国内统一刊号,国际标准刊号,邮发代码,定价,投稿方式,出刊日期) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (url,title,abstract,zgbm,zbdw,bjb,zb,zr,dz,post_card,phone,cz,email,journal_url,cn,issn,post_size,price,ways,date)
    try:
        curser = db.cursor()
        curser.execute(sql)
        curser.commit()
        print('%s插入成功' % title)
    except Exception as e:
        print(e)
        print(sql)
        time.sleep(999)

   