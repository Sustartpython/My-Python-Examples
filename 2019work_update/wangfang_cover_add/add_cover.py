import requests
import pypyodbc
import os
import time

# mbd数据库
DBPATH = r'E:\work\wanfang_cover_add\封面更新统计_20191015_1.mdb'
# 链接数据库
db = pypyodbc.win_connect_mdb(DBPATH)

# 总路径
path = r'E:\work\wanfang_cover_add\cover'

def do_sql():
    sql = "select bid,years,num,gch from 汇总仅无封面"
    curser = db.cursor()
    curser.execute(sql)
    result = curser.fetchall()
    return result

def down_pdf():
    results = do_sql()
    print(len(results))
    for item in results:
        name = item[0]
        year = item[1]
        num = item[2]
        gch = item[3]
        url = "http://www.wanfangdata.com.cn/sns/third-web/per/perio/magazineFulltext?id=%s_%s_%s" % (name,year,num)
        print(url)
        # 测试
        fname = path + '\\' + str(gch) + '\\' + str(year)
        file_path = fname + '/%s.pdf'%(num)
        if os.path.exists(file_path):
            print('已存在')
        else:
            res = requests.get(url)
            if res.status_code == 200:
                if res.text == "":
                    line = name + '\t' + str(year) + '\t' + str(num) + '\t' + gch + '\t' + url + '\n'
                    with open('err_url.txt','a',encoding='utf8')as f:
                        f.write(line)
                    print("无期封面")
                else:
                    fname = path + '\\' + str(gch) + '\\' + str(year)
                    if not os.path.exists(fname):
                        #创建文件
                        os.makedirs(fname)
                    file_path = fname + '/%s.pdf'%(num)
                    print(file_path)
                    if not os.path.exists(file_path):
                        with open(file_path,'wb')as f:
                            f.write(res.content)
                        print("下载完毕")
                    else:
                        print("%s已存在" % file_path)
            else:
                print("%s错误"%url)
                break
            

if __name__ == "__main__":
    down_pdf()