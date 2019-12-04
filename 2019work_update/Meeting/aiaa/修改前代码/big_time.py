# import toml
# import os
# from parsel import Selector

# kong_count = 0 
# count = 0
# listPath = toml.load('config.toml')['listPath']
# fw = open(r"E:\work\Meeting\aiaa\big_time.txt",'a', encoding='utf-8')

# for parent, dirnames, filenames in os.walk(listPath, followlinks=True):
#         for filename in filenames:
#             file_path = os.path.join(parent, filename)
#             print(file_path)
#             with open(file_path, encoding='utf8') as f:
#                 text = f.read()
#             html = Selector(text,type='html')
#             temp = html.xpath("//div[@class='teaser__item'][1]/text()").getall()[0]
#             line = file_path + '\t' + temp + '\n'
#             fw.write(line)
# # 
# # import time
# # fr = open(r"E:\work\Meeting\aiaa\session2.txt",'r', encoding='utf-8')
# # count = 0
# # for line in fr:
# #     if line != "\n":
# #         count +=1
# #         temp = line.split("•")
# #         session = temp[0].strip()
# #         times = temp[1].strip()
# #         print(line)
# #         if count == 2841:
# #             time.sleep(120)
x = "August 12-14, 2013"
m = x.split("-")
print(m)
endtime = m[1]
ss = endtime.split(",")
print(ss)
ye = ss[1].strip()
print(ye)