import requests,time,pypyodbc
from parsel import Selector
from urllib.parse import urlencode
import toml


class bookspider(object):
    def __init__(self):
        r"""
        __init__重写，初始化
        """
        self.count = 0
        self.baseurl = "https://ascelibrary.org"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
        }
        # 读取配置文件获取数据库地址,链接数据库
        self.DBPATH = toml.load('config.toml')['Dbpath']
        self.db = pypyodbc.win_connect_mdb(self.DBPATH)

    def get_url(self,page):
        r"""
        get_url()函数为获取列表页，调用解析函数
        :param page: 页数
        """
        url = self.baseurl + "/ebooks?"
        params = {
            "pageSize" : "20",
            "startPage" : str(page)
        }
        url_one = url + urlencode(params)
        print(url_one)
        try:
            res = requests.get(url_one,headers=self.headers)
            if res.status_code == 200:
                res.encoding = res.apparent_encoding
                html = Selector(res.text, type='html')
                self.parse_url(html)
        except Exception as e:
            print("first error : %s" % e)

    def parse_url(self,html):
        r"""
        parse_url()为解析函数，用parsel库，调用存储函数
        :param html: 规则化后的html
        """
        try:
            book_names = html.xpath("//h2[@class='itemTitle']/a/text()").getall()
            links = html.xpath("//h2[@class='itemTitle']/a/@href").getall()
            for i,item in enumerate(book_names):
                self.count += 1
                book_name = item
                #处理标题里的单引号和access的插入语法冲突
                if "'" in book_name:
                    book_name = book_name.replace("'","''")
                try:
                    link = links[i]
                    url_two = self.baseurl + link
                except:
                    url_two = "未知"
                y = "//div[@class='listing-view']/div[@class='listBody']/div[@class='listItem odd clearfix' or @class='listItem even clearfix'][%s]/div[@class='rightSide']/div[@class='itemFooter']/span[@class='year']/text()" % str(i + 1)
                years = html.xpath(y).getall()
                if len(years):
                    year = years[0]
                else:
                    year = "未知"
                arr = [book_name,url_two,year]
                print(arr)
                self.save_info(arr)
        except Exception as e:
            print("second error : %s" % e)

    def save_info(self,arr):
        r"""
        save_info()为存储函数
        :param arr: 该列表存储目标信息
        """
        curser = self.db.cursor()
        sql = "insert into books(bookname,url,years) values('%s','%s','%s')" % (arr[0],arr[1],arr[2])
        curser.execute(sql)
        curser.commit()

if __name__ == "__main__":
    r"""
    启动程序，调用bookspider()类
    """
    begin = time.time()
    book = bookspider()
    for page in range(20):
        book.get_url(page)
        print('***JobStream*** this,time cost: %.2f s' % ( time.time() - begin))
        print('第%s页完成' % str(page+1))
    print('***JobStream*** current time:%s ... \n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))