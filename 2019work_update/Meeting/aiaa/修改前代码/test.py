

import os
import datetime

# 年-月-日 时：分：秒
now_time = datetime.datetime.now().strftime("%Y%m%d")
path = r'E:\work\Meeting\aiaa\big_json'
os.chdir(path)
pwd = os.getcwd()+"\\"+now_time 
big_json_name =pwd + '/%s.big_json' % (now_time)

# print(pwd)
# 文件路径
word_name = os.path.exists(pwd)
# 判断文件是否存在：不存在创建
if not word_name:
    os.makedirs(pwd)
with open(big_json_name,mode='w') as f:
	f.write("111")

