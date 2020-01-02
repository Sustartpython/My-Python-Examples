import requests


# url = "http://www.360elib.com:2100/chinese/web/PageList.aspx/bookList1_Show"

# data = {'page': '2','id': '0','childid': '1','desc': '','strwhere': '','types': ''}

# res = requests.post(url,data=data)

# with open('1.html','w',encoding='utf-8')as f:
#     f.write(res.text) 

url = "/chinese/web/PageList.aspx?id=0&childid=1"

import re

ll = re.findall("id=(.*)&childid=(.*)",url)[0]
print(ll[0])

# url = "http://www.360elib.com:2100/chinese/web/PageList.aspx/bookList1_Show"
        # info = re.findall("id=(.*)&childid=(.*)",g_link)[0]
        # id_ = info[0]
        # childid = info[1]
        # data = json.dumps({
        #     'page': '1',
        #     'id': id_,
        #     'childid': childid,
        #     'desc': '',
        #     'strwhere': '',
        #     'types': ''
        #     })
        # res = utils.get_post_html(url,data=data)
        # if res:
        #     html = Selector(res.text)
        #     maxpage = html.xpath("//span[@id='ctl00_ContentPlaceHolder1_maxpage2']/text()").extract_first()
        #     print(maxpage)
        #     if maxpage == "0":
        #         utils.printf("%s 分类无图书" % g_name)
        #     else:
        #         fdir = "%s/%s/%s" % (list_path,now_time,g_name)
        #         if not os.path.exists(fdir):
        #             os.makedirs(fdir)
        #         page = int(maxpage)
        #         for i in range(1,page+1):
        #             HEADER = {
        #                 "Accept" : "application/json",
        #                 'Content-Type' :'application/json',
        #                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        #             }
        #             data = json.dumps({
        #                 'page': str(i),
        #                 'id': id_,
        #                 'childid': childid,
        #                 'desc': '',
        #                 'strwhere': '',
        #                 'types': ''
        #                 })
        #             res = requests.post(url,data=data,headers=HEADER)
        #             fname = "%s/%s.html" % (fdir,str(i))
        #             with open(fname,'w',encoding='utf-8')as f:
        #                 f.write(res.content.decode('gbk'))
        #             utils.printf("%s down success" % fname)