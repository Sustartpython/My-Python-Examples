import json
import toml
import utils
import time
import pymysql
# 读取toml配置文件
DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']

conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)
result = []
path = r'E:\work\PNAS\big_json'
sql = "update detail set stat = 0 where url = %s"
for _,fname in utils.file_list(path):
    print(fname)
    with open(fname,encoding='utf-8')as f:
        while True:
            line = f.readline()
            if not line:
                break
            dict_ = json.loads(line)
            info_htmlText = dict_['info_htmlText']
            with open('1.html','w',encoding='utf-8')as fs:
                fs.write(info_htmlText)
            time.sleep(200)
#             print(url)
#             result.append(
#                 (url)
#             )
# utils.parse_results_to_sql(conn, sql, result)
# print("插入剩下%s条成功" % len(result))
# result.clear()
    


# path = r'E:\work\PNAS\1.txt'
# with open(path,encoding='utf-8')as f:
#     while True:
#         line = f.readline()
#         if not line:
#             break
#         print(line)

