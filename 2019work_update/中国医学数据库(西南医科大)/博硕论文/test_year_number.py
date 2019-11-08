import requests
import time
from parsel import Selector
from queue import Queue
proxy = {
    'http' : '192.168.30.176:8127'
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'Referer': 'http://www.sinomed.ac.cn/lw/basicSearch.do',
    'Cookie': 'JSESSIONID=A038BD20E8FCA5A19AFA7F259795F621.sinomed; com.trs.idm.coSessionId=A038BD20E8FCA5A19AFA7F259795F621.sinomed; _trs_uv=k1r6y6pw_784_4grg; _trs_ua_s_1=k1r6y6pw_784_kpai; uuid=1571109964378.0056; refreshedTimestamp=1571109581172; history_lw=history_28773043-1_28773066-2_28773104-3_28773177-4_28773368-5_28773386-6_28773395-7_28773401-8_28773404-9_28773420-10_28773422-11_28773432-12_28773436-13_28773451-14_28773457-15_28773465-16_28773470-17_28773481-18_28773490-19_28773613-20_28773900-21_28773905-22_28773931-23_28773941-24_28774008-25_28774029-26_28774043-27_28774392-28_28774432-29_28774439-30_28774447-31_28774450-32_28774459-33_28774488-34_28774576-35_28774583-36_28774624-37_28774634-38_28774643-39_28774698-40_28774704-41_28775137-42',
    'Accept': 'text/plain, */*',
    'Host': 'www.sinomed.ac.cn'
}

count = 0
url_que = Queue()
for year in range(1980,2018):
    url = 'http://www.sinomed.ac.cn/lw/basicSearch.do?dbtype=lw&pageNo=1&pageSize=100&change=true&cmode=&flag=&time=&linkSearch=&more=&moreExp=&searchmore=&searchword=+%22{}%22%5B%E5%87%BA%E7%89%88%E5%B9%B4%5D&submitButton=&beginDate=&endDate='.format(str(year))
    message = (str(year),url)
    url_que.put(message)

while True:
    if not url_que.empty():
        try:
            message = url_que.get()
            year = message[0]
            url = message[1]
            res = requests.get(url,proxies=proxy)
            res.encoding = res.apparent_encoding
            fname = r'E:\work\SinoMed_tasks\博硕论文\html' + '/' + "%s.html" % str(year)
            html = Selector(res.text,'html')
            totalnum = html.xpath("//input[@id='itemTotal']/@value").extract_first('')
            if totalnum == '0':
                print("%s年无文章暂不下载"%year)
            else:
                allnum = int(totalnum)
                count += allnum
                with open(fname,'w',encoding='utf8')as f:
                    f.write(res.text)
            print("%s年有%s文章"%(year,totalnum))
            print("一共有%s文章"%(str(count)))
        except Exception as e:
            print("停止时年份为%s"%year)
            print('停止时count的个数%s'%str(count))
            url_que.put(message)
            print("重新添加%s年到队列中"%year)
    else:
        with open('个数.txt','w',encoding='utf8')as f:
            f.write(str(count))
        break

