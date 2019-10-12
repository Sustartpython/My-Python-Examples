import requests
from parsel import Selector
import re



class Demo(object):
    def __init__(self):
        self.url = "https://www.cdggzy.com/site/JSGC/List.aspx"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
            'Host': 'www.cdggzy.com'
        }
        self.count = 2
    
    def get_first_page(self):
        res = requests.get(self.url,headers=self.headers)
        if res.status_code == 200:
            html = Selector(res.text,'html')
            __VIEWSTATE = html.xpath("//input[@id='__VIEWSTATE']/@value").extract_first()
            __EVENTVALIDATION = html.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first()
            html_page = html.xpath("//span[@class='active']/text()").extract_first()
            print("当前抓取的页数为%s" % html_page)
            self.post_html(__VIEWSTATE,__EVENTVALIDATION)

    def post_html(self,__VIEWSTATE,__EVENTVALIDATION):
        data = {
            'ctl00$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$Pager',
            'ctl00$ContentPlaceHolder1$displaytypeval': '0',
            'ctl00$ContentPlaceHolder1$displaystateval': '0',
            'ctl00$ContentPlaceHolder1$dealaddressval': '0',
            'ctl00$ContentPlaceHolder1$keyword': '',
            '__VIEWSTATE' : __VIEWSTATE,
            '__VIEWSTATEGENERATOR' : '9F052A18',
            '__EVENTTARGET' : 'ctl00$ContentPlaceHolder1$Pager',
            '__EVENTARGUMENT' : str(self.count),
            '__EVENTVALIDATION' : __EVENTVALIDATION,
            '__ASYNCPOST' : 'true'
        }
        res = requests.post(self.url,data=data,headers=self.headers)
        if res.status_code == 200:
            html = Selector(res.text,'html')
            html_page = html.xpath("//span[@class='active']/text()").extract_first()
            print("当前抓取的页数为%s" % html_page)

            # 返回的__VIEWSTATE和__EVENTVALIDATION信息在一个div里，用的|分割，需要用re判断
            patter = re.compile('__VIEWSTATE\|(.*)\|8\|')
            patter2 = re.compile('__EVENTVALIDATION\|(.*)\|0\|asyncPostBackControlIDs')
            __VIEWSTATE = patter.findall(str(res.text))[0]
            __EVENTVALIDATION = patter2.findall(str(res.text))[0]
            # print(__VIEWSTATE)
            # print(__EVENTVALIDATION)
            self.count +=1
            if self.count <= 30:
                self.post_html(__VIEWSTATE,__EVENTVALIDATION)
            else:
                print("抓取%s页结束" % self.count)

if __name__ == "__main__":
    demo = Demo()
    demo.get_first_page()


