import requests
import pypyodbc
import toml
import time
from bs4 import BeautifulSoup as bs

class ipspider(object):
    def __init__(self):
        r"""
        重写__init__,初始化
        """
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Host': 'www.xicidaili.com'
        }
        self.proxies={
            'https':'192.168.30.172:9172'
        }
        # 读取配置文件获取数据库地址,链接数据库
        self.DBPATH = toml.load('config.toml')['Dbpath']
        self.db = pypyodbc.win_connect_mdb(self.DBPATH)
        self.count = 1

    def parse_url(self,url):
        r"""
        完成对页面的解析，抓取有效的数据
        :param url: 爬取的url
        :return: proxy,address
        """
        try:
            res = requests.get(url,headers=self.headers,proxies=self.proxies)
            # 判断网页响应码，确保数据有效
            if res.status_code == 200:
                res.encoding = res.apparent_encoding
                # 格式化该res,便于bs解析
                html = res.text
                soup = bs(html, 'lxml')
                try:
                    # 分析网页源码得，tr标签中class=''或class='odd'都满足
                    Tags = soup.find_all('tr', attrs={'class': 'odd'}) + soup.find_all('tr', attrs={'class': ''})[1:]
                    # 获取该页所有的代理IP
                    for tag in Tags:
                        td = tag.find_all('td')
                        ip = td[1].get_text()
                        port = td[2].get_text()
                        address = td[3].get_text()
                        proxy = ip + ":" + port
                        self.save_ips(proxy,address)
                except Exception as e:
                    print(e)
            if res.status_code == 503:
                print('ip被封')
        except Exception as e:
            print(e)

    def save_ips(self,proxy,address):
        r"""
        存储抓取到并处理的数据
        :param proxy: 代理ip
        :param address:  来源地
        """
        curser = self.db.cursor()
        sql = "select count(*) from proxies"
        result = curser.execute(sql).fetchone()
        if result[0] == 200:
            sql = "delete * from proxies"
            curser.execute(sql)
        sql = "insert into proxies (ip,address) values ('%s','%s')" % (proxy,address)
        curser.execute(sql)
        curser.commit()

if __name__ == "__main__":
    begin = time.time()
    ips = ipspider()
    for i in range(1,3):
        url  = "https://www.xicidaili.com/nn/" + str(i)
        ips.parse_url(url)
        print('***JobStream*** %s,time cost: %.2f s' % (url,time.time() - begin))
        print('第%s页完成' % i)
    print('***JobStream*** current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
