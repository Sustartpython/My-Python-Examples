# import toml
# import os
# from parsel import Selector

# kong_count = 0 
# count = 0
# listPath = toml.load('config.toml')['listPath']
# fw = open(r"E:\work\Meeting\aiaa\session2.txt",'a', encoding='utf-8')

# for parent, dirnames, filenames in os.walk(listPath, followlinks=True):
#         for filename in filenames:
#             file_path = os.path.join(parent, filename)
#             print(file_path)
#             with open(file_path, encoding='utf8') as f:
#                 text = f.read()
#             html = Selector(text, type='html')
#             session = html.xpath("//div[@class='issue-item__session_details']/text()").getall()
#             url = html.xpath("//div[@class='issue-item__doi']/a/@href").getall()
#             for i ,item in enumerate(url):
#                 print(item)
#                 try:
#                     print(session[i])
#                     line = session[i] + '\n'
#                 except:
#                     continue
#                 fw.write(line)
# print("没有eISBN的个数："+str(count))
# print("空eISBN的个数："+str(kong_count)) 
# 
import time
fr = open(r"E:\work\Meeting\aiaa\session2.txt",'r', encoding='utf-8')
count = 0
for line in fr:
    if line != "\n":
        count +=1
        temp = line.split("•")
        session = temp[0].strip()
        times = temp[1].strip()
        print(line)
        if count == 2841:
            time.sleep(120)
        