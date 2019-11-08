import requests,re,json,time
import pymysql,datetime
conn = pymysql.connect('192.168.30.199','root','vipdatacenter','nantong')
cur = conn.cursor()

#写死的cookie
cookie = 'JSESSIONID=16A411F0038AAD2600431F726266AED3; DWRSESSIONID=Zs75AkVDeklh2le3rjnkMklBOOm'
scriptSessionId = 'Zs75AkVDeklh2le3rjnkMklBOOm/TOSBOOm-f5*pq2Sqe'

#获取报纸数量
url1 = "http://app.xunku.org/dwr/call/plaincall/EpaperCatlog.getSpellSources.dwr"
a_z = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
    'Rerfer':'http://app.xunku.org',
    'Cookie':cookie
}
# for paper in a_z:
#     data1 = {
#     'callCount':'1',
#     'windowName':'c0-param0',
#     'c0-scriptName':'EpaperCatlog',
#     'c0-methodName':'getSpellSources',
#     'c0-id':'0',
#     'c0-param0':'string:%s' % paper,
#     'batchId':'38',
#     'instanceId':'0',
#     'page':'%2Findex.jsp',
#     'scriptSessionId':scriptSessionId,
#     }
#     res1 = requests.post(url=url1,headers=headers,data=data1)
#     te1 = res1.text.encode('utf-8').decode('unicode_escape')
#     pattern = re.compile(r'r.handleCallback\("38","0",(.*)\);')
#     newpapaer_list =  pattern.findall(te1)[0]
#     paper_json = newpapaer_list.replace("id:","\"id\":").replace("logoName:","\"logoName\":").replace("name:","\"name\":")
#     try:
#         paper_jsons = json.loads(paper_json)
#     except:
#         continue
    # for paper_info in paper_jsons:
    #     Id = paper_info['id']
    #     paper_name = paper_info['name']
    #     sql = "insert ignore into paper_name (id,name) values (%s,'%s')" % (Id,paper_name)
    #     cur.execute(sql)
    #     conn.commit()
while True:
    sql = 'select id,name from paper_name where stat = 0'
    cur.execute(sql)
    sql_data = cur.fetchall()
    if sql_data == ():
        break
    else:
        now = datetime.date.today().strftime('%Y-%m-%d')
        for i in range(1):
            times = (datetime.datetime(2010, 7, 9) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            # times = now - datetime.timedelta(days=i)
            print(times)
            for info in sql_data:
                Id = info[0]
                name = info[1]
                url2 = "http://app.xunku.org/dwr/call/plaincall/EpaperSearch.getEdtions.dwr"
                data2 = {
                        'callCount':'1',
                        'windowName':'c0-param0',
                        'c0-scriptName':'EpaperSearch',
                        'c0-methodName':'getEdtions',
                        'c0-id':'0',
                        'c0-e1':'string:%s'%Id,
                        'c0-e2':'string:%s'%times,
                        'c0-param0':'Object_Object:{sourceID:reference:c0-e1, day:reference:c0-e2}',
                        'batchId':'40',
                        'instanceId':'0',
                        'page':'%2Findex.jsp',
                        'scriptSessionId':scriptSessionId,
                        }
                print(name)
                while True:
                    try:
                        res2 = requests.post(url=url2,headers=headers,data=data2)
                        if res2.status_code == 200:
                            pattern = re.compile(r'r.handleCallback\("40","0",(.*)\);')
                            newpapaer_banmian =  pattern.findall(res2.text)[0]
                            paper_json = newpapaer_banmian.replace("anchor:","\"anchor\":").replace("id:","\"id\":")
                            paper_jsons = json.loads(paper_json)
                            print(paper_jsons)
                            for paper_info in paper_jsons:
                                anchor = paper_info['anchor']
                                Id = paper_info['id']
                                ontime_id = str(times) + '@@' + str(Id)
                                # try:
                                #     print(ontime_id)
                                #     sql = "insert ignore into paper_banmian (paper_name,anchor,ontime_id,id,on_time) values ('%s','%s','%s','%s','%s')" % (name,anchor,ontime_id,Id,times)
                                #     print(sql)
                                # except:
                                #     sql = "insert ignore into paper_banmian (paper_name,anchor,ontime_id,id,on_time) values ('%s','%s','%s','%s','%s')" % (name,'编码错误',ontime_id,Id,times)
                                # cur.execute(sql)
                                # conn.commit()
                            break
                        else:
                            continue
                    except Exception as e:
                        print(e)
                        err_now  = datetime.datetime.now().strftime('%Y-%m-%d---%H:%M:%S')
                        err = '报错时间:'+str(err_now) +',报纸id:' + Id +',报纸时间：'+ str(times) + '，错误原因：' + str(e) + '\n'
                        if " 由于连接方在一段时间后没有正确答复或连接的主机没有反应，连接尝试失败。" in str(e):
                            continue
                        else:
                            with open('err.txt','a')as f:
                                f.write(err)
                            break
        break


    
