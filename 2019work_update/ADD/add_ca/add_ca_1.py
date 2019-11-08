import os
import re
import toml
import time
import redis
import random
import requests
import pypyodbc
import pymysql
import datetime
from queue import Queue
from parsel import Selector


class add_ca(object):
    def __init__(self):
        self.url = "http://cb.cnki.net/journal/Search.aspx?pid=&key="
        self.headers = {
            "Accept" : "*/*",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
            'Referer':'http://cb.cnki.net/journal/Search.aspx?pid=&key=',
            'Cookie': 'Ecp_ClientId=1190726160601495301; cnkiUserKey=2d5bf84e-f876-893d-c4a9-e73de6a5f95d; UM_distinctid=16d14991c7a2a1-0dbe3f4fa34fef-c343162-15f900-16d14991c7b667; SID=202188; LID=WEEvREdxOWJmbC9oM1NjYkZCbDdrdVRVQ0Zhdm1lU2RKUklJazdwbGM1WGM=$R1yZ0H6jyaa0en3RxVUd8df-oHi7XMMDo7mtKT6mSmEvTuk11l2gFA!!; Ecp_session=1; HidSelectField=; CNZZDATA3001634=cnzz_eid%3D1475022117-1568882558-%26ntime%3D1569023088; ASP.NET_SessionId=fqllxh55tqet044503mhjyns'
        }
        # 读取配置文件获取list网页存放路径
        self.now_time = datetime.datetime.now().strftime("%Y%m%d")
        self.list_path = toml.load('config.toml')['list_path_1']
        self.list_path = self.list_path + "\\" + self.now_time
        if not os.path.exists(self.list_path):
            os.makedirs(self.list_path)
        # 读取配置文件获取mdb数据库地址,链接数据库
        self.DBPATH = toml.load('config.toml')['Dbpath_one']
        self.db = pypyodbc.win_connect_mdb(self.DBPATH)
        # 读取配置文件获取代理列表
        self.redisHost = toml.load('config.toml')['redisHost']
        self.redisPort = toml.load('config.toml')['redisPort']
        self.redisDb = toml.load('config.toml')['redisDb']
        self.r = redis.StrictRedis(host=self.redisHost, port=self.redisPort, db=self.redisDb, decode_responses=True)
        self.proxieslist = list(self.r.smembers('proxy_acs'))
        # 读取mysql配置
        self.DBHOST = toml.load('config.toml')['DBHost']
        self.DBPORT = toml.load('config.toml')['DBPort']
        self.DBUSER = toml.load('config.toml')['DBUser']
        self.DBPWD = toml.load('config.toml')['DBPwd']
        self.DB = toml.load('config.toml')['DB']
        # 链接mysql数据库
        self.conn = pymysql.connect(self.DBHOST, self.DBUSER, self.DBPWD, self.DB)
        # 创建队列，将解析出来的详情页url存入
        self.detail_url_que = Queue()
        # 创建队列，将list页的页数存入
        self.list_num_que = Queue()

        

    def down_list(self):
        r"""
        解析列表页，ajax，post请求，202页
        """
        for i in range(1,203):
            self.list_num_que.put(i)
        while True:
            proxy = {
                'https': random.choice(self.proxieslist)
            }
            if not self.list_num_que.empty():
                num = self.list_num_que.get()
                data = {
                    "__EVENTTARGET": "ctl00$body$AspNetPager1",
                    "__EVENTARGUMENT": str(num),
                    "__VIEWSTATE" : "dXZlfv0t+s4RgjHxfTMXC8oZSLheA6KXHHrbovnYXR2OmU0H2hnFFGqRdwzrAIVDTRlyQ3S0qFJ5XLNCryPfHsbo52R6+zi9BA5WADfV5xWsG1IJL0RbMvN0Mau7j2Ex5pMWpO+T+9LD45cLMiUl9nYccHQXYnLnW7VnHdPvOmIkKYHK2SBOyMM2RUJy0j/mcNW9IPXcruiM/PcBqQFVuOXPoWAqn7PZ+spNwp9z4lNbnYKYDdgq+rlTI7yWS9nj9fdZ3II57ux3qSrdsdyKxV9RnWucpT3VNRYVgMKLwwdhfTa1wrMD+RwlY2ErDAmtaVLSv78OBnAleYdWofKgFHd4APd8yeodvvUOEMeNgHWBB6eCQpTyd4eDNyVsXEhtgFSt8abA55O5Akj4hVuFdekjZpVuD8g5Q04+bK7on4txBqAuRxi3Eo31Hqzm63iQxI/dmcgyRHsjyzF5+u6yEu79Xlx8bLE5khREQ0DEhHweDVFjMPZSLgFG1Lafe6JlCwnhTmEdcSF+CWEjrBd87dPphniNIMeFSZOcHMq97QJk1snA7dk76aEo16FKcLXnMi9IJ+rcAtMApYMimutnlVIU1/K4bHlQ5rMTYy1iV/7PO+32oYG8F/HVEAFuKgyFvEh1jsEEeU4rOT9NJN9jmQiTo8oCEp+qHZ1rpYRe0LZdzNJkp0upva9qVr70wig+XCqJtV+hLcLXE7b1OlI8ICcRR+EO5OXUE2I/Zj22H9L7kaeLdQr2/gmUMLSSmwJ49DEWNuBYSB0b23O+mTj8Ei90PcKbE0GzD4bhzoggYjETVfDw8upLh/k6WJQtQ/vx2lVKnB1v/qnhbwZL3s2YhriNoDVfrxv2SrRyUgge0eSgs8jBtj6Pr3zohk3btVoqHU7IcfK/vciXtUDB8Kt1W9XsK9dFRQ5JFJ4pGcymUtiuMQjaPiWU1ilRSb9Y/D2RUUTQGZhEcXfltVTYtoonJhuSanFEgxvPLImnW+vDlFf2d+z4Wtju4At/H2t/DxqZvl8h8frHNVVHuPpV1rIwgiIbbyzRfKcLPBWMt+hFEShJMufZzUR6FB2/RI8CmLiE3b5O89IgxJc8GNvl7M5h1MAW18YH8ZFvAeMC3IbIjy7xwQTY4WRzxhRNEHAADQO+cBzzFUdVNqpsrCgFAp+1wHQQ4wgBcdv8PHnpIHm6D7vZBxJtfMWbWBLQhMz1gyCW4xa83sxh3LF6x7nelpflSfMmpJdiRZWxd5SLyHMM0fNvvt7eqyCpyHGD4I/Wst9iwpSqpSVpnY73nVZekdwsejPZErzNlIBDZwKTzFlIh0DkBnIRZRVhSFtNFNG2y9tgN59CYf+p24iWd4Vw7r1RgxGre1+Vad0PqteZ/np2GdCdSa3+/9kAtl/p4tS+fXgB/hE+Y/QkCzD2QnAqPHw6sz9doih11C0esKXUaCFrX27vD/6uIGF0RnpXuC+8PLMqwGK6MR1wRS1y7P3OxMKezcOz+2kuv+P4hfFhgcDjm9/eu4nhpNRgDgpyi7iFcrlEfmRC6vu0ErdLE3p7+IdIZirUSEwZhjJMY5a3P5qWkyuceF9g41VJIF3wnjxGVGZ5a5z12JezDnQngehWg5fIg1VnwEcDyEnlD7UqXuJTxTlTwlWAmh7v2MT3CxFkYgCy4sPZkHN8FExo4T4lsOcu6Ojbqd8Ze2spyDNhnM4zU8vUT11vZ2RUP8+teh1hW88EXsxcD9tZJfqd0TURpYyPNHEhZkzcdWV+m+6CEIsmuFTyl89UDMPzEy0d9BYnlQ6ipgecMlGlQLfMqWnKOmCRMYaORykdj6jA0ANPDxzDfIsX6QWcNtvIWP1pVwDGoHFtGJ29aA7DqVNDMdjPR99pGzc4Tr/63CsYv6O2bibtCMZ2o7BTtBEhV2wt/hudPRgTzZS2YUZfut+BARi0cRtKjhuQbDFJn0RLJRiVYHosJ6MWXtl33tSi1JnHGUtj8nBHuD8ze1p1NuS8+Yx6Z+gErb44Z1NUSL6+bhCXwNF3xEHXiqEbb/7sFdgmrt00zOiMSLjj0/K3VWo+DuvxVd6DCFaqRXrCOnzdDGdeJQd6xJAUmIzvc5XvU2rSiiWjMB53KE/AonnC/Cuv9BYqKu32tTalvTm4M3z/kctsQf/p17xdXN12RAdag9BrCHdKmaLUqynLVa/WAX+LrDX2rw3A5rC0WmyT29nEUPyR3RIi957x+k8UQRx0QKfTi4Af7tEQvRQV8paAhHMF+kXBuclxRlsBjUEcVBIvM3PU09fni5QhtGi4SZvrO2nhofx2yNvaIVsfjFV3IBvnR3DvWejQ1GvRcjslxXQbE3sEK2bXzj6c5JmUAFUK2sdJB0ob/mol3jb4tq6V3Wp5fzANyGVMwOrJGfqTPVkPUp/vfsw3ZyKQSAv81phkdr9FrYD9Hno5J2DW+hllUSZpc5Zkmv4UHAee70eYNp4c6cd12Ppz/eS87Q5Vr4rhc9i/pXJPFpOfY31tAWaveamRRTD9xsBAb35NQZoey0FaYwnFAOlPY/tfZj/28eO2p43gGQm1NJyqIXrC7Fd840oTm2qxdjKtbKBVHgSxztLfPL/+9xclKGbJGt0UBkNoNS3KjbsJZZgT9bUPDzVhbSkA0U/UKtXhipNonbAKkrGRNGKdw/SZjQMOBffwplTuaXMoQbSZUJ5FtIR7uHTKnQKxdvvWp0wrdszMbwIkS+us3lWqNwrSST7n7H7RWTBwIS5/ilNdvKhwD+VbGG1id3EzwXz6kQEtB1i63pu48RrQjEPvgzz0ywB852LbZzowndAPyEKx0UJJ4JAwIM/KGlqiYxemgBDASe++YJq/o+2t/NNK77kJMuJKPUcLTAWn9WLqop3z0PVmUy21m949yjmPKZtBcVVntKoNKmXIsz4V31Iq8vr+6DNCwXwSDgvBNnCoQCDpUfdm+tnc61scsPvszkwdWsm2sarjGVyGg4oH6D+sm57iba0Yd1ZRasbVLj3uL9TPVPaG4ek9vyQ3bgF9kX1Z/danxY/eC8OC9L+d4oyQu/MlFNXR1puM7BL44e8zRvZmwYalDA+L7pCXnpgDuRSsyAKd8GCJlbW+MQg5n64NsNLyF4CXNG5NlpsvXKQvcJROgpLxBVw326WggXlf4g/gNpTOBq0WAWi9HWBs2Z6b15S+jPP6FzXzrX9Vhy5kjMjp53Dg7DQneg3QhvvaESrP0hsPDNUODYkRKdoi+Rep+hTOL9H8pukvD6tsw2noGN7ErwgwNqICdIKOanPWAffmI1Ks6Phxhz9wi8yodRdyckF5CnphpPUE2Qr8vJrtDpvrafxORcvlXwA50BDoBX8uNmrn+SR0H7QPxDdBuTQJGJSxJgsQLnu1UeYXxcx6SBwI2E3vCrxiqDhugl07Mmq/JhwZTqc/QFE1Rtyv7r1/6eNlsUoe5ub40ScPsX8b9biQXOSiOhe9wPz3ojtXM27TjZWvopOC6yN8bAWe0xr8ZAvkxvvAMXj3wFPZRFsRtenQPVMs+sfeEBiXvGMz8vQYr/kq15sXTFC8p16FGtqrITj09YEdqD7lDvzee2JzNhaiE6tFCVUZTHVYrLxCDwaISsHWKs+nSvUBBZ1I487K7AUw/Ipzy2K1kIo7xMPgHOUHdnDqdr312wR8bz1Km0/sRKoC3zGDSkwGZoFEFaOnW0d4mHh0hzm+bIOTxhRSNv1cWyXvmnOjpH5Fn9HHyVQte5oawRHBG8OyU5WIS+A+gE7AinC5iocjgGb9NStTMg4/JXPvg8f7xmKd7j7q+1MHTEmOUN4b7S55I1P+pGQD8Si5e8xB52jwemz7RBiSCYyVPiI9mYvv/HP7VKIBPxkkgUHMXwd5dZebHMSPE94LrHigQR2G632BEbCq6LTNqlSVNWrG+3k/1+0QKcsgFj2G3PrZsX2QYtSCNapGA8zKNLfxPfRYNSr86+StGY9UJjA3ac9povpYg71fRX81hbJ4kPG1TQaekEd/3q4N+Ty5da6ffTeMXFRk+H+lYeqe2QeXiQLEQ3rF6+m0v11ntszwSdMZCqb+w86FrX9858R1oMpFugvIXatgen823wtw5J/ugc2eNukc9y4I7aaWtGskzX9+yn/eiO3O6kTEUeEkOmpJex+dtfiP3hW1/+LfwUW6PfcqhNjocs5EfL1foLARR1+2ntTY1stPCFMDWwxnUMfUv7etn4tfdp3zuneLlbopTGgA2nUSv+usgdtAabzEMNhYxxkoa+efKnyPwM/4nceT1LU+IlYsmiVBmN6AbJBGTCX7GgJpodmJcZsWMrFJOiHV1nNaREQxbOOWp089O0yNvay70X9kkvy3cteKXAhIg5lTkrQNxD22E/be/Rn1hKq3A/97+6OIXTxHhuFhiBmq3zYDNWyudSUmqu4q0xZ1utBMus12qEDnsgr6BpotIfy3sSc9zUzWgRn5HbW/iwW145+WXfoZnvqMQmkrLplctACUGW9dtbJDgEp6mnoEYbIkv7rukG43yjo22//XyldPWJUun3AM6HBII4rMC/zBtLjEa/jC2Lf835I/THicQajbzqnpDrYHAy/QajWTsOgAEJZ2nTUggV4YdWD0IrEXYjIXHmlZum2qffQhX6Akwvr4lKev0HsWboEJJu8wMj/wpI1N84cyRIUUioIdAAbLaDwqshnAHg/Gdq7xcF1gX7wSOm6180SqkrPbnMbXdjntCtsbjyMFnFhGQWuisWnYAl2sayQtvT0/N6j34G0X2Irj8Aue7wTAaaia3quBYrJOTAOtD4nk+NuBoQlgvfXN47tzum/z7TonN2GKkffEQn+C0XVe3IXQjB1fTUKiDWATuqry5VaPiLagNJ0OCBcrxC01HABXoZbjEIxzjK4CWYbG9ixhO/nDUC73gggsWm2ssahwU1m8Fc7T9nQ8WpVgAzy+l33zPPpLC9l+9FtIKvFvwC57eUQzwmnjkTiG2A8Zubd294yVhpQXWNP7Ysd+xoVQFrb+Ja0fI5bjvLXCvFAcmR++0biTnjBPkrfr4gKAxHmMy0oyULPi5wPi1s/vrMELSOVo3eygstnT4Bp9PeKY+3wTDIYLxBGR0Be3GJZE2LqmwV8lFXZzdzshsqIXW7HiDjjmn4memivKij7wY2VpVBUnLDTPGuwn0ncPkWyIxej4Z9DZmJVQw9OmFvA8zY0Ap0nLlDGe/sG+BL1Hy3FwhMPVk4EQhlyefOv6Lrdz07Zw++GhtLaLSg3qdwopf7sP6MQr35/Bjv0tgo4ISj1zUN6Q9O4png/snHBKqI1VUJfXBB/0nwRu3TAJo08G/6MEgEtbjbMl26wCqNBCdnEn7QrmlLeQKQJG/wdcalclocz2w0OXpEA42sDwD1OOVl5S7Q/TpIKVsu4pYQY3Q84dJXEbUZQQtjiLFhTXrzFmVDOTzHOHPYKTUfYTkQFod5DN3mJGzA3KUHnJFajtvDIW7TbM+ZxwpT3KSYTHPuRsRybFsSd6X5Jv/F1KkzZeBfk4JbzSkHgnB0NVDFeHbyBIYgiSzFyKw/fDcgmYqaH6Vs9beC3mTFcQZQ6trMhngOTtBt2yUklbqb4U9DLW9ko1h60KhoThsfUvnAk0sNfWs0tKYsD1EDiUiXROxt8dY8XvbMwXBPR+mI1u65poPOasVuNDlFmYRBPSGzZ+JGN4xUYmu7wRwycU7UHaJmdUnROOL6WzGFtOKP68go9c6df9hWmn6cqMqa0X3IWJpQJWk4yOklMD1x8FcFQgYZsTC0057kV5fda4p8clhsYVNVh/l0kgL3j6VylL0G+L3UVKtFAeuyNTDWHvme5rCKQAGNhU+orj75ksa5PxoepUmrqERkYgD3FcwDg4KgeYCUiuCzEE/3C4Te4ioi/ZDZiu2JrsjNhZ2a/B4Q+PWosizN5NtkeGRl+R/xoGvrOkTgFjR/xiGk0jEyvkM1yDNs7sYKDPHbuHZrgvjjX0AljJDyOYiffh4HEnKriIEJJXgORE/gdGfifVBywJdsbMEBQq8zXep5G1XXXu3Np608Ox2OiC86wJG82R3IoxeR5EBzqNvyah6hIMzZ/o/tuNz+qYR0pgSjy5yd5Sede076nHp+lrH8tY2f9U9IxgR+9bWgZWgFx6T96qaFMBpcQTLWjFpCZEo3E/SoIAFYcXBdc/Hfe8kieLqHwe3kzUYJbxTnjcIVUWEwbgfmvylHJyWcaTyHyV/zx7//7oxZIyV9uQIiFg1Nw03U7VQMxWp4zTzlTmuz9zc/9AxyWdosUF1bMo6NASwZmM7rFxsEEU76EnfUUarSXNEiIXdzp/qLEDoKftkMZsXBi+TFd5WF/0QAb0tqYQwcHuZsy2OMoqDj/svseIqF+jQr8o5DxVI8W+ceM+NQd87Y7po1gBDWqJ9m1u/2wB7qbri9tUAc1qjxhHIkrbUalgjxllEhY5bZFhK/FSvqirBoDeBnrkXGAgnlBYlsGM6vV/dXCRlKAUbfyjPtuffsPsDDDVaepOwirH++dNjSy7CYdVYND0e51pZQJ0rfN2K838D9xzIBnIBK8egrtukuafF5DVZCaPHvRgNZwrqvqyOZmxTp9SUu2M2plsPh/eJfuHbCIAnXxrWjO+kmwWjQ5S6KNXQ3kvXrJL0lEMUz9YiSIlE73HFwHs3LVP7SEiZnlOF4uX6OW5KugaiKrOUJMgmdWuNJBMWuaGBTQ6UiyLAU2tiiceZqQ0AGkxJKvCVKv9ykW35wef4MT9nfulQXvQ/EDY+o80vpxwqjxZMJboBOgc3SyS3cdDZs/yGEfVXG0ZdoaHqEdcq2bmAfIdRanIixAfPB/c18nUsjIZ1jWtpJbf2ArEZ3rK1UQ6+yxRQCZqdLcdvtZzoWs13GzGY3tBG9cXsR4keAd7W1ssf7lEsBiWnt7BO+QVmfPu7GB8freZrnmIjIQFFuAJnWyxGBuksXRpGcS3aWINE+hrIWJxBAlPbk6u9n0QTKzyVKKmk+ykMdeJU/I7kiz1WV10Kt+i6gk7X9dgNhueaDVROy/qYDCaE/Vp3WdDo1cZdlIEZ/qgKvX4aQWOCXlGqE2C4OMVFmQnfECMacDiLXacPOCarsWiOeB46pFMTtQ+zYBvJ/NfvrsXa3w/J8WWaG2gcOi3p68dyfrFuhFEpf/WLNctEPke9Mj7r2dEn9P1aLN2REtdRJZaMUBmrx43VIJotYjutSk/nelQW54ozGMLUj0F4PfWO4zqiZIQ2AlZ2JYtAy3ECGVyZm70+6VwSeGocs8LE/032tBOMm2R4uSOIa7U33JxJlyXLciH/FZDuyi25eJe9Ax7TvAvkMiTDNZ2rEV1wZEq0acQ3/mWJmu3bdD/yfRoYg2eds8G4WgUgZdOxEccNfMLzv1nKKiEXeeiK/w9KQHMfC46jbLTaDCQq5CyOv7dPZWnNKyGOO+5p6JkboUxmxIm7dfTfjPcKfpO917VTzEJkq8DlHDAYdNwbGmfdvPZrm9Kj9JHbQdTk44sV/bI+CdSltEBZGKZANwaX2Ib2lTol1N5Y51qxBkZSJDKA7FShnwpiZTLV8HCcqnE4fEPP4GYPtlli63t5qRT2CmSJ/HwCeMRNgiEOC5LSsjQ79KHmWkw+4TqohDmSXPVQDikYw+Mb5ooQuUlNdyDKM68Rd1TsNcRKaat98WjY1duxhUgInCjknnnxheVh/fdEIfR5kmGGFcGeNuknIRVT/l67oK1emi7rw812lUPTrAt1G4P41HZQ89OZ/Zkk4fV5o8DEjPAr9S+2OE0JwdsYrlRtfN8t15aFveuca3u6yBmvU9q45QGGxYTOrmoHPgE8gBe8JyICxaa9H7ZJ68L4hVQvojEUz2snc7fv8RScL34+AMGYDnwVRQGlG1K+gr7EVpYMrA5opncVcOjzvvbrDPZUkIJ2cLBR7WCJdmpS8jb35ntcqNkM/s/IhSnFToP66Z/GqSLDpZbOXYsOWiboBt9zajQY2/uLgVtJ/PhGT/y627qrJNGAzenw+EFxzQWuWKFJZGcf5rCb88D+yeOLAd5Yz0yVc03OTRc1P7I/wNpQgVwqZDZH/wVxkzdPxV7M44HvK7ofbxAVEjfe1pHn3d7sgUrX78ySMi0pDDjQFCueeMYMFBzY+25ehpUFrrPxhv751OPGMjTc+PYxyYj56u+i/JXIgTUqJXCve6WGZ58rf7Jc6aWx7JghbFD9gyd+9l2v45Y+ZlYV/TIsaQDgHZoHtJnMOF5qbT71OWpKut83AkfEVRNuuvHBubvxI+agwiP2m19526PP/KC/3A6f955ClLp9Byzm9e4kllZlqbSyQT0RGjCTrxaVhBkEPS8jUptLFiug2ca4RiPkrlrnpg7o35jSWDnXxpD1QXUz5OYOx5iCpOX3ay8+SFwfwp8P2iOCtsF3GO606XP3PfV1Hm8cQeHqeAmzHrlWwlYVJxEpcJDgqglgqwh7dFi1U/KRt/IMt5IxC80Wj9DqqarixJxuOvomDRga9WNqpRTStcqDQp4U9wWkMX/GR1Z40ET00lNQWfpJ2FQMo+XU5VxI8CzqgJteOYWzS+rgyQwNunG+WATYxnOwJPu8r8sSU5WBEOyBdF7q8MsT6j7BBgUMB9OjGw6Jl4n7TuMslMgztVCdvsMsLxA41K8PA0N+LdTKwphZS2g5w170mZbdXp8xDZ/CZ542Q/gahpZ9rhDZQiz4xQrkCMq111eoxBwt0nsKK3UYHUxVnX5A255m/r7bVdneHYYKy6kbRgCxAbl5Qrx/FN+lcGqA/2Yfc+p3lso93lsFmdKUg3DNllfIzMYYnfzL9dFgGiIT/jbq3mZh8RxjDpRjolTk77cbr9VXseuk/a70BzG7vrtmzZ9Th6kNmp/Ud5bLD9hhFKS1+yEcO5XGVaykEGFXf9eheTk6pV3ur/nRIiOj2ENsoemgFp41Oa8a4ErDU214xlTGlNTJxyhZivLHCjRfdrgYEQMhINkrrSqbrariDDImJZPq+lduGVxv/6MDkKknKsdN0w8rU5lPz0hgQiKRDuscSeNY/E4+ZDcSG3AjKN5aBr2TfnUhbGVgNO6e9+mkzAvikj1cnvmDWt5Sv7LJ15HyA96/xYFErcBDVvG0Gi3O3tdbYO3wJLbWEtWy6Hahf+GLQCWL8A1orGF65sWDoY5g8f3B0EnCoEYmf4SVJPNfZIkbDcEOJZqtE81uRxTj4fvYWj0q1pmm1e83e6s+VR7+YNYQObuQSNfYAa+Jr91QQLOsSTGkyCNrfw47ep4Zvh/eryyG97zbAOVgGsayt6X+WP3hCDVGrERcQwQr184deEUToF/UpqDVG9VuK9ZQwKEpx0R12B/ZiOM4tsCBT4KahgT1tYZMadw/gxJUXy4Zv42bkwT5w2uOuuSwzmDbLxaajwxHZgT81PyjEPV4Ffs6ZUnPqI8g3GwQTFFqnJaVVog4ZBvwSj6BteZvMlQpyTxVepRaUYYT9Xc1luVNhp/L/soT4lOqSkPMBK4MZZtiQ2KFWCej7p+/ldMeOvqckSCg2ww59eMP/cW8R8th0HxZ0noPu9peU4vySN4VFfJKo93lirq40exJUDB6UXgz7vvYtmcrrbjl0Zl2Hg+FCOiHv7zfZSKxbj0lF5KdwmuKX96v+3er94f2J1sjWYdFxuA4pfiwfrGKThLELhhzwJ8fAhKebArYlehiYfuu6D0zyiLHJOSfKqbwW8Us0RB2XDTu/iVLVIofCbe2HaLud491aJkhMhZuGQfh3x6pi96HH7YSISBbi6/09CHyCrtUesjOMcMCkNvN62plEyIZ8hZjXRvTlkj4/31PiHC3oaYXcnO+TAfvpldyXIif14USIIBFQFFIY8YIikqMSTKY17HEPJIdKRVrI68jLcb6j/mcWSznYckOuO3f//TG/cg1rmil/cILgO86qgO3I1PM4/gGJLVOqJr4OzrkwnthWx4QA+vpwuEKMSl9OZ02haSDmAu0ONeg2GTqs1tIpgUAuTcgiB88zWrRw/LT7USPPC7klM0KHGWCQXsbwSCqmsi+DY+DkvsNj24alXZ2G8HX2DM7Qz4NkFVOxXpgscUJHupynkWn9jOZ0gQCkk1dyGU3pMJOdnk2n4mXfWWoHZEEstWD7PcGPYaRHqW5Zbd0BWFlvfQ24TYeDaMLn+cfgV5arjO3hkNdnjN6P1m+HeTU4+1bUgMf2l2zK2OKB8jw7R10FDNmDb38vo98qFXUZPYxpShR+Z5DJu4QXMboo25CDok5gRFuczuIA2ccWtLB1kQ8ORbSpUOjwv3UE1SIYlu5VO6QoSirG6WIgq9OtDHfRkdcwhBCM/VYSwOBsL+ESS/iobFRaoQzNGElQ+1ETXrKQHcf2UCClE4oKmNBwj3cm2ufvt/t70YPq9olz6H7A5rC+pC/mVUvPS/z6EL1HT31Vk5d7K1UoJ6RAHIoLXFfpYP55xeRHbAf2De1YJfWiVUJ+FsOGMmxOmsUtAYTNi4HbJ81wLGmi8+/0Ms6hE11Sygh15H4+cQTEY/XyjrGlGtLENDNv6jThWhQQy/zSMoAQFJPWhAHMvqyQJ753CvjKvk7Pserjr3b7Vni813QylZGGjOcThlGv5zub+ll59hGGMWqCqKQsPNYiLLGVPqddJcfpic6KDlESfOIyATgy20AgHp/yH+tvueqjXJRSUSpN9yldc1Xe06LbKbgKJfzLH+qEBLWE4MNz3KfCzzf3azZWNLo6mN6n0q1Ic1L4JSwB7HnoOYtlvd1tpTH19Wa7YWrgOTXzGs18+1xFjAF7LBJsJhO8TRZ1l/ijlHDVfQZlhadJoRLVmVrXuFAXTw5IiPfxpKi0u/mvEQY42bjfq7TGBLl45qOd/l2PtwLDYgTW9UM6UQCK1AYNUgeXquQcqKGVZREQ2swM8YE4FzYNC99lt7vnxoeakOWzAdwtuJzR4xwa8JDmE+3vcSI7gAcTnJTfvVCsS/IU11pvgog/e2ZITqprD2vhqThC9b3w35n1yc5sEGCKxXvns+CSMUY9CYzKPhvOOXVdXGAA39AygMORzvOzlAIGmOGtTryAmHncFFsxXq+Sazf7hgu20TUaK/iHm01SmEgLnEeJpiM7SS2yhXEYu5RvRZ4ots1KkmV2nMFP+lXvmznQ6bhdG4RvG3LLREuvKM+dIawg8LLXlvj/4CHpin3Y/PQkTQ2nW+WQDoRoZA1MNwek/L9q80N8Ec20meqT6uPJZoWbi2Po+Z2sVOXnKl1Y15tbMUUPrz3VUIdwjwWdaEO/3znd5RwtGbqa9m7iMir+TafKSTN10zlUI3uCbT9FXnm8vf18CrRx5uVA4fjLSnmyng8r/Vy5Tb4T6KpiMdWoXIp0kMq5LitQTGn0bxKgxUTtuXEFrTIkKwmR/vAxIyxbbvCpc8gVr3HdkF7YIFBAu+w4n+lOIW27Vb7ZLDUg5huQ1JBoWyzHFs+HW13t9eB8xQGHYLkaPjD8DzVWGRst1LBcORO4guLFNfpujp7l3Y193JMrFYgkW9r5h4ZDMpgOhT1yBpG4v6M/yvVChEAjVRLUI0v2YrTDaVE5MXur9de0A2crZyXe1henDVAfsk5OmDNhnH9PYhntJLOcPKHYQH1nbdT3sroxqutNA1KdS0K+f1JpyM+vyLFdl8epO0zj5LLY8MYLGEpXSR+Ugod+FRD3AJGGP36nVeU2nMRHaCVeywd99BOi/iHJOEL3eEtYEN8FI+6WJ6rbfDtDOhYLCySCjxhwbRDIOpTkVKTbJblfjHm+nU9TXEtheMOvh1m3YCZKfR6Q5bqG3XEgHaw0ksHGnjEDd/rlhquShcB5jMKFK9sYLC1dL86BRVRaUqbXMLTIZHrOiohvbBZ5vGfkHUqTaFCLcDOGDc2BKzGgtc8nviV4JIBFfBEdpjKyN++VThtGexEFAOlfDhjkwv/F74j2qVMJk+BCJG07S+2ksInc/nBr4pW+w/7oiGmp/wwdbfiVlntxEurANwX1LhU15ct5ae0cdHpKuE0fw+9+SlGTSr1KXuLLm9BVqy2zZ/1cF95pmkWzpeTe+gDdHV5bcn1gkC6Lk2LWCM/BYIGgpjh10bq7s29vD/yXis0OSyTyOl18/7n8eJ2+0vPIExe6KNQpLLsHJFa2DaU2Jcb07RE7BvCvuKfGDVImUAJI3ehkd4/YuJX8wj4V+95RxzgMbxJ/4D+S77lybFyhP6mt5ie6EoCIVtlICFFF3OSXBN98m2SBP29wzF6Ou8uS6bOQNZ5OC8Q4VOy9iy+NmoOxXfUuKdJjLaD97pWbBwjqdCGboxWz1Ex5VFpLBfv8oaNsUOUdt2UtEVU5EQuyGUqN5A4Ik2ug74cs+VbFOcXgzvRypK/c6JsSGfKGBzc+CkBMwnb3uIB46kgwttDV9fUihr395/e/+uo1dw0HkSF8HmIPIzC64NoU7KPmZ0rHbQgZ0yVE5dESAGcJFffzgRixM8ZaiTrk7hXec0Hkx7j2aFNP13m/Dmx683l+Kl9SzQtNXye8XSkCIU6OLlodYvVqXTuN58bEL1djUVxv4nHiB7JhOnIjvIVYNgaCrRXkYRw22wC3asBGe9OaeRDErRo7CEcTH8k9ZQ9KBsVUTltp+Ev5wiqCPD/ob3yWks+BaXm6FVwjzpSrkKTnRRg4ymxJD2uYdx31+c9UUzU04sDGkS0JjRssYjfhYiOFBvkVtZ0ZiIBR52EsM2HKmwdTTO0AVAQkrqyI5G8idDJjHAxcpYlx8epeXAYZ+QWOv04IrB2943nPEX6/G/Ky5Y6DjAffRTmXwSbczoz5RnB7RgQNwJHcTd3R7eO8UiApH58B5zydwT+fKWActi+4SXMaog5odvYbvAmKCBzvNOKBiypX85PoD3LTKd85KKSRhWQgHYNLOzzaqKMFOskH9ttiG2QToE0j9Rwa+0+MIPbCvKUj45mcrw5m0D8a2B4rHa8kEYCzKvfZ/aX5E9z6QSAPFb1p8IFEDrf9i9f+GCiV3hzrVYCHC/e3sBV6C9OouVLqB/Volvwgn51pfOGxEsEvKldMzCiHBiwMvmzjXTM4i8t/BxG8AmRwFUxwUAUq4LRx08EGc3zciFWP0frPAnBLsj37hqoh/BWY6DHstYM1TbCyZKzi09tAHjbxsrRSZ+2xSZ7s0j01ct+m5aWjExfGgeBzn6NyR3oJ/leIfdPM43apQgZeeDV+gG564SmvpIsy/Ik76VylNxcM0D53S0ievNBBceBbFKePoNw8WGqLjqP0/i56Bcek9BDP87gCsKBcAYB2A+MoMLptyDoPLbHsKVMCn8gIDKbLDK5tC7DhGh9mb6woiIIWvjf02zfGhrQgSIo7LXxWQ9Moa9gB/P0dnqEok9jqH3LFI7W0VBPCq8pAA9M5iOXwr5Wjtx3fhvM5dBdO4EUbFh8g6y9ZTZg1m4KDqqVrvbYIgAPYIoYeuptBebBBe+j7eryNZCefIWKaJyaOJpFsUB2xYfntdS+qxBMq95s6lc6NCQ4GgmJvot3GhYjo0ySC7iG6p1/g+1X8ZQsK2vv4FWNx5L2bPwpuDMf+xJ48dNqAZOTw3f5VKoOEGUDInQSw9xDuh5seZ4xgxC7EipyFfUaJae/Zgw1exrDi5c2Fj5sMvwGhqQwgHHVbc0jiuiIrdPuwJ/ld3kIdIBc+/VtXYFy+v6NWiXx31HBOEM0NoWmu1mBK2/Kn55Z/ge4LMGAT3gPrRYMKymsFvfAcZcxj2KPVN4oJcrcUSe930LG3tcqXQp1+RX2OTmAgCtrqh0pYrGIljUxCSO55ZhvXTlpBUzh1vbcSy0NaKO1ZxtFzTeoGFw0YawET7DaBRuT0udsbtBdZTLSXGNdAsx8QLkMhdeOzXjGvNPlD5XEVTTvHzEDTWm4PzenoNBrN89F5nAVtLKotdO4KTQ8Xwe3yotVPQHZD6nb+F+2ZHI0RaGHSLeRIGF7S0TTb6+XNIvPXO50Ytdx7wNswO19vwwpZ7RWmjctA0/n47dfQZaQ5kZtS3RWnKHO86ZqjDCWrk6fYvwkf+LYQFM15ozsovYpV52/m68qBrdc/IH46sMHxdTBj0yNNy8FsqOvmtDam8YuWhDN5EtBWpl1fOzK2f4Dx4yxfCTg0wEE0ZMB9CqheAdqoePDruh4Zg+LrwfTKlUAEv2mwKk09EIC0eJKLu/v8E+TGNrYH7vv6SXR/bVItfwQXwtBB8I7eeHilW6/2xAp8V6ve39dxvDgDWAegvmzwBN9wzFtXaI702SZgIbEBg8ypXCnCMmE3tyCsvQEOxXYibRVGJKjarzLWZfNFAD3E90R4rPrBQbrjQ9ULt/986+5wGCp5tdWp8QMrc60pP8OrqMYPOCbUhR/2ytgaLUkJvwjaNr5wx1tnuI8lUqsxDQnQT95WvKxlLhH/XWWq95Q3VNVRK69JWvTKfn4GkitAHB/VtvdVlNExuAeu2tnqVbTpD49qx7Blqk4uRw+AQSA3K9zN1VhBT8UAMhq+NA8Er8vFr4tx2bDklTIpuRpZjNHcC5BY5d6v0GotqeH7YOKiz6RN+3YzS/Bty+I6L41cthGZ7WWBJRWyvDlUk2b2KkFzn8sWInpMH3FQ6yy3EVWeWhJeaX5lTwHZWG7Iq85ENrmSb93sB/jMc2iA3fD8kxBVEVPSYrAvLSJ0DXXiT2IZbJEnePrl4GGtrRclMl5ximDPyU/K0iLbvr92Z/M4ed/gOmcqBjggodvh7LE3WeFvS/is0GCwEUS+m5VNaHVUwwIg0fBUyMAQiOcakOwfuF5PGdCUI0MOx9HBuc/6NROzEvjY6NWGRlSzjzpAL58uTQ5CX2z8QoeuoVJgLqcYAudA70H/P9lcq7yDCmN4ce2aya2LvrOUqxQSb8muULMUCVlrXzs6U5IyTXq+vR17jm2ov9kWCBkDsdWZiaVIIF89bsvecs4M2xI39cawOvRG7CTXqMmf4GERlG62Jd40Htx2gKxGjIy42GcQAL7PORLrHZ7E5dl7q1IxIas2b8cJ6xUKMHdg9lLTIPbuMURHPtJc9c3R2pwxyHn7sVmc6VBY1VFHoZTBStndxOdlxUZCl5zI3aXMzkOF2Aw7/L9Ik618h2guQqRT5g9JAnQgOZKON2sipl/lqvAx49i6oq5yfRTeyiPZukZT9JvT+lMHw+oPvJA6MuICYfMvCZiOojt9HQNWyCfkA0tYpPBrR+HmuMWWOlpubVIE29rWNsXq4mhdXSbqZ9Yu2cqKoAASmksiB7KSuwG663BZyPKacorR0sb7zIweIT8xGtlhg4zyg4aKHf270ILkf9wnvj4shoTvbd",
                    "__VIEWSTATEGENERATOR":"662BA313",
                    "__VIEWSTATEENCRYPTED":"",
                    "ctl00$Search1$searchWordTxt" : "请输入投稿方向或刊名(多词逗号分隔)" ,
                    "selectFilter" : "Influence",
                    "selectFilter" : "Desc",
                    "ctl00$body$HiddenField_OrderField" : "Influence;Desc",
                    "hiddenCheck" : "",
                    "ctl00$body$AspNetPager1_input": "1",
                    "ctl00$body$HiddenField_SelectField":"", 
                    "ctl00$body$HiddenField_SelectJournalIDs":"", 
                    "ctl00$body$HiddenField_PageSize": "50"
                }
                path = self.list_path + '/%s.html' % num
                if os.path.exists(path):
                    print("第%s页存在"%num)
                    # self.get_detail_url(path)
                    continue
                else:
                    try:
                        res = requests.post(self.url,headers=self.headers,data=data,proxies=proxy)
                        if res.status_code != 200:
                            print("%s页重新添加到队列" % num)
                            self.list_num_que.put(num)
                            continue
                        else:
                            with open(path, mode='a', encoding='utf-8')as f:
                                f.write(res.content.decode('utf8'))
                            print("第%s页缓存成功"%num)
                            self.get_detail_url(path)
                    except:
                        print("%s页重新添加到队列" % num)
                        self.list_num_que.put(num)
                        continue
            else:
                break
                        
    def get_detail_url(self,path):
        with open(path, encoding='utf8') as f:
            text = f.read()
        html = Selector(text, type='html')
        title_list = html.xpath("//tbody//tr/td/a[@title='查看详细信息']/text()").extract()
        link = html.xpath("//tbody//tr/td/a[@title='查看详细信息']/@href").extract()
        for i,item in enumerate(title_list):
            journal_name = item.replace("'","''")
            url = "http://cb.cnki.net" + link[i]
            sql = "insert ignore into add_ca_one (url,journal_name) values ('%s','%s')"%(url,journal_name)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        print("该页链接插入mysql完毕")

    def get_info_2_que(self):
        sql = "select url from add_ca_one"
        cur = self.conn.cursor()
        cur.execute(sql)
        sql_data = cur.fetchall()
        for url in sql_data:
            url = url[0]
            self.detail_url_que.put(url)
            print("%s插入队列成功"%url)
        self.conn.close()
    
    def get_data(self):
        count = 0
        while True:
            proxy = {
                'https': random.choice(self.proxieslist)
                # "HTTP":"192.168.30.172:9172",
            }
            if not self.detail_url_que.empty():
                url_one = self.detail_url_que.get()
                try:
                    print(proxy)
                    res = requests.get(url_one,headers=self.headers,proxies=proxy,timeout=30)
                    if res.status_code == 200:
                        count +=1
                        res.encoding = res.apparent_encoding
                        html = Selector(res.text,'html')
                        # 期刊名称
                        title = html.xpath("//h3/text()").extract_first('').replace("'","^").strip()
                        # 期刊名称（翻译）
                        title_en = html.xpath("//ul/li[2]/span/text()").extract_first('').replace("'","^").strip()
                        # 主办单位
                        host_unit = html.xpath("//ul/li[3]/span/text()").extract_first('').replace("'","^").strip()
                        # 信息表格
                        table = html.xpath("//div[@id='basicInfoDiv']//tr")
                        for tr in table.xpath("string(.)").extract():
                            # 创刊时间
                            if tr.startswith('创刊时间：'):
                                create_time = tr.replace('创刊时间：','').strip()
                            # 刊发周期
                            if tr.startswith('刊发周期：'):
                                journal_cycle = tr.replace('刊发周期：','').strip()
                            # 期刊简介：
                            if tr.startswith('期刊简介：'):
                                journal_intro = tr.replace('期刊简介：',"").replace("'","^").strip()
                            # 主管单位：
                            if tr.startswith('主管单位：'):
                                gover_body = tr.replace('主管单位：','').replace("'","^").strip()
                            # ISSN：
                            if tr.startswith('ISSN：'):
                                issn = tr.replace('ISSN：','').strip()
                            # CN：
                            if tr.startswith('CN：'):
                                cn = tr.replace('CN：','').strip()
                            # 开本：
                            if tr.startswith('开本：'):
                                book_size = tr.replace('开本：','').strip()
                            # 语种：
                            if tr.startswith('语种：'):
                                language = tr.replace('语种：','').strip()
                            # 邮发代号：
                            if tr.startswith('邮发代号：'):
                                post_code = tr.replace('邮发代号：','').strip()
                            # 国内发行单位：
                            if tr.startswith('国内发行单位：'):
                                in_body = tr.replace('国内发行单位：','').replace("'","^").strip()
                            # 国外发行单位：
                            if tr.startswith('国外发行单位：'):
                                out_body = tr.replace('国外发行单位：','').replace("'","^").strip()
                            # 发行系统：
                            if tr.startswith('发行系统：'):
                                system = tr.replace('发行系统：','').strip()
                            # 状态：
                            if tr.startswith('状态：'):
                                stat = tr.replace('状态：','').strip()
                            # 中文核心期刊(北大)： 
                            if tr.startswith('中文核心期刊(北大)：'):
                                chinese_core_journal = tr.replace('中文核心期刊(北大)：','').strip()
                            # SCI： 
                            if tr.startswith('SCI：'):
                                sci = tr.replace('SCI：','').replace("\xa0","").strip()
                                if sci == "":
                                    sci = "否"
                            # EI：
                            if tr.startswith('EI：'):
                                ei = tr.replace('EI：','').replace("\xa0","").strip()
                                if ei == "":
                                    ei = "否"
                            # ISTP： 
                            if tr.startswith('ISTP：'):
                                istp = tr.replace('ISTP：','').replace("\xa0","").strip()
                                if istp == "":
                                    istp = "否"
                            # Medline： 
                            if tr.startswith('Medline：'):
                                medline = tr.replace('Medline：','').replace("\xa0","").strip()
                                if medline == "":
                                    medline = "否"
                            # CA： 
                            if tr.startswith('CA：'):
                                ca = tr.replace('CA：','').replace("\xa0","").strip()
                                if ca == "":
                                    ca = "否"
                            # 中图分类号：
                            if tr.startswith('中图分类号：'):
                                chinese_number = tr.replace('中图分类号：','').strip()
                            # 期刊荣誉：
                            if tr.startswith('期刊荣誉：'):
                                journal_honors = tr.replace('期刊荣誉：','').replace("'","^").strip()
                            # 载文量：
                            if tr.startswith('载文量：'):
                                article_number = tr.replace('载文量：','').strip()
                            # 基金论文数：
                            if tr.startswith('基金论文数：'):
                                article_supported = tr.replace('基金论文数：','').strip()
                            # 基金论文比：
                            if tr.startswith('基金论文比：'):
                                papers_proportion = tr.replace('基金论文比：','').strip()
                            # 被引频次(CNKI)：
                            if tr.startswith('被引频次(CNKI)：'):
                                use_times = tr.replace('被引频次(CNKI)：','').strip()
                            # 下载频次(CNKI)：
                            if tr.startswith('下载频次(CNKI)：'):
                                down_times = tr.replace('下载频次(CNKI)：','').strip()
                            # 浏览频次(CNKI)：
                            if tr.startswith('浏览频次(CNKI)：'):
                                look_times = tr.replace('浏览频次(CNKI)：','').strip()
                            # 复合影响因子：
                            if tr.startswith('复合影响因子：'):
                                double_reason = tr.replace('复合影响因子：','').strip()
                            # 综合影响因子：
                            if tr.startswith('综合影响因子：'):
                                all_reason = tr.replace('综合影响因子：','').strip()
                            # CNKI学科分类：
                            if tr.startswith('CNKI学科分类：'):
                                cnki_subject = tr.replace('CNKI学科分类：','').strip()
                            # 审稿周期：
                            if tr.startswith('审稿周期：'):
                                review_time = tr.replace('审稿周期：','').strip()
                            # 发稿周期：
                            if tr.startswith('发稿周期：'):
                                pass_time = tr.replace('发稿周期：','').strip()
                            # 录用率：
                            if tr.startswith('录用率：'):
                                enrolment = tr.replace('录用率：','').strip()
                            # 审稿费：
                            if tr.startswith('审稿费：'):
                                fees_money = tr.replace('审稿费：','').strip()
                            # 版面费：
                            if tr.startswith('版面费：'):
                                page_money = tr.replace('版面费：','').strip()
                            # 稿酬：
                            if tr.startswith('稿酬：'):
                                article_money = tr.replace('稿酬：','').strip()
                            # 地址：
                            if tr.startswith('地址：'):
                                address = tr.replace('地址：','').replace("'","^").strip()
                            # 邮编：
                            if tr.startswith('邮编：'):
                                postcode = tr.replace('邮编：','').strip()
                            # 网址：
                            if tr.startswith('网址：'):
                                url = tr.replace('网址：','').strip()
                            # 联系人：
                            if tr.startswith('联系人：'):
                                linkman = tr.replace('联系人：','').replace("'","^").strip()
                            # Email：
                            if tr.startswith('Email：'):
                                email = tr.replace('Email：','').replace("'","^").strip()
                            # QQ：
                            if tr.startswith('QQ：'):
                                qq = tr.replace('QQ：','').strip()
                            # 电话：
                            if tr.startswith('电话：'):
                                phone_number = tr.replace('电话：','').strip()
                            # 传真：
                            if tr.startswith('传真：'):
                                fax = tr.replace('传真：','').replace("'","^").strip()
                        sql = " insert into data (url,期刊名称,期刊名称（翻译）,主办单位,创刊时间,刊发周期,期刊简介,主管单位,ISSN,CN,开本,语种,邮发代号,国内发行单位,国外发行单位,发行系统,状态,中文核心期刊（北大）,SCI,EI,ISTP,Medline,CA,中图分类号,期刊荣誉,载文量,基金论文数,基金论文比,被引频次（CNKI）,下载频次（CNKI）,浏览频次（CNKI）,复合影响因子,综合影响因子,CNKI学科分类,审稿周期,发稿周期,录用率,审稿费,版面费,稿酬,地址,邮编,网址,联系人,Email,QQ,电话,传真) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') " % (url_one,title,title_en,host_unit,create_time,journal_cycle,journal_intro,gover_body,issn,cn,book_size,language,post_code,in_body,out_body,system,stat,chinese_core_journal,sci,ei,istp,medline,ca,chinese_number,journal_honors,article_number,article_supported,papers_proportion,use_times,down_times,look_times,double_reason,all_reason,cnki_subject,review_time,pass_time,enrolment,fees_money,page_money,article_money,address,postcode,url,linkman,email,qq,phone_number,fax)
                        curser = self.db.cursor()
                        curser.execute(sql)
                        curser.commit()
                        print("第%s条,%s插入成功" % (count,title))
                    else:
                        print('ip被封')
                        self.detail_url_que.put(url_one)

                except Exception as e:
                    print(e)
                    print(url_one)
                    self.detail_url_que.put(url_one)
            else:
                break


        


        












if __name__ == "__main__":
    ca = add_ca()
    # ca.down_list()
    ca.get_info_2_que()
    ca.get_data()


        

