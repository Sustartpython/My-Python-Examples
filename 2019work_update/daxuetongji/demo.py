from parsel import Selector
import time
import pypyodbc

indexpath = r'E:\work\daxuetongji\index.html'
dbpath = r'E:\work\daxuetongji\双一流大学专业统计_20191115.mdb'

def parse_index():
    db = pypyodbc.win_connect_mdb(dbpath)
    dict_sum = dict()
    with open(indexpath,encoding='utf-8')as f:
        text = f.read()
    html = Selector(text,'html')
    p = html.xpath("//div[@class='TRS_Editor']/p/text()").extract()
    for item in p:
        xx_zy = item.replace("\u3000",'')
        if xx_zy == "":
            continue
        xx = xx_zy.split("：")[0]
        zy_all = xx_zy.split("：")[1]
        zy_list = zy_all.split("、")
        for zy in zy_list:
            zy = zy.replace("（自定）","")
            if zy in dict_sum.keys():
                xx_ = dict_sum[zy]
                xx_ = xx_ + ';' + xx
                dict_sum[zy] = xx_
            else:
                dict_sum[zy] = xx
    for key,value in dict_sum.items():
        sql = "insert into data(学科名称,建设学校) values ('%s','%s')" % (key,value)
        curser = db.cursor()
        curser.execute(sql)
        curser.commit()
        print('%s插入成功' % key)


if __name__ == "__main__":
    parse_index()
