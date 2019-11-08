import requests
import time
import toml
from parsel import Selector

class oalibspider(object):
    def __init__(self):
        r"""
        重写__init__,初始化
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        #读取配置文件获取写入文本地址
        self.TXTPATH = toml.load('config.toml')['txtpath']
        self.proxies = {
            # 'http':'192.168.30.172:9172'
        }

    def parse_url(self):
        r"""
        解析url,并调用存储函数
        """
        url = "http://www.oalib.com/lib/showJournalList"
        try:
            res = requests.get(url,headers=self.headers,proxies=self.proxies)
            # 判断网页响应码，确保数据有效
            if res.status_code == 200:
                res.encoding = res.apparent_encoding
                html = Selector(res.text, type='html')
                journal_names = html.xpath("//div[@class='paperlist']//li//a/text()").getall()
                link = html.xpath("//div[@class='paperlist']//li//a/@href").getall()
                for i,item in enumerate(journal_names):
                    journal_name = item.strip().replace('\r','').replace('\n','').replace('\t','')
                    url = link[i]
                    info = "%s\t%s\n" % (journal_name, url)
                    self.save_info(info)
            else:
                print("ip错误")
        except Exception as e:
            print("first error : %s" % e)

    def save_info(self, info):
        r"""
        存储为utf-8的文本文件
        """
        with open(self.TXTPATH, 'a+', encoding='utf-8') as f:
            f.write(info)

if __name__ == "__main__":
    r"""
    主函数，调用oalibspider()类
    """
    start = time.time()
    oalib = oalibspider()
    oalib.parse_url()
    print("this,time cost: %.2f s" % ( time.time() - start))
    print("current time:%s \n" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
