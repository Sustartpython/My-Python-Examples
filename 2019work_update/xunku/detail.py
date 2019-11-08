import requests,re,json,time
import pymysql,datetime
conn = pymysql.connect('192.168.30.199','root','vipdatacenter','nantong')
cur = conn.cursor()


#写死的cookie
cookie = 'JSESSIONID=16A411F0038AAD2600431F726266AED3; DWRSESSIONID=Zs75AkVDeklh2le3rjnkMklBOOm'
scriptSessionId = 'Zs75AkVDeklh2le3rjnkMklBOOm/TOSBOOm-f5*pq2Sqe'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
    'Rerfer':'http://app.xunku.org',
    'Cookie':cookie
}

while True:
    sql = 'select paper_name,anchor,id,on_time,create_time from paper_banmian where stat = 0 limit 1'
    cur.execute(sql)
    sql_data = cur.fetchall()
    if sql_data == ():
        break
    else:
        for info in sql_data:
            paper_name = info[0]
            anchor = info[1]
            Id = info[2]
            on_time = info[3]
            down_time = info[4].strftime('%Y-%m-%d')
            url = 'http://app.xunku.org/dwr/call/plaincall/EpaperSearch.getEdtionDetail.dwr'
            data = {
                'callCount':'1',
                'windowName':'c0-param0',
                'c0-scriptName':'EpaperSearch',
                'c0-methodName':'getEdtionDetail',
                'c0-id':'0',
               'c0-e1':'string:%s'%str(369782),
                'c0-e2':'string:%s'%str('2017-03-02'),
                'c0-param0':'Object_Object:{edtionID:reference:c0-e1, day:reference:c0-e2}',
                'batchId':'51',
                'instanceId':'0',
                'page':'%2Findex.jsp',
                'scriptSessionId':scriptSessionId,
            }
            # while True:
            try:
                res = requests.post(url=url,headers=headers,data=data)
                if res.status_code == 200:
                    pattern = re.compile(r'r.handleCallback\("51","0",.*newslinks:(.*)\}\);')
                    newpapaer_detail =  pattern.findall(res.text)[0]
                    print(newpapaer_detail)
                    detail_json = newpapaer_detail.replace("anchor:","\"anchor\":").replace("hashCode:","\"hashCode:\":").replace("orgUrl:","\"orgUrl\":").replace("url:","\"url\":")
                    detail_jsons = json.loads(detail_json)
                    # print(detail_jsons)
                    for detail_info in detail_jsons:
                        print(detail_info)
                        
            except Exception as e:
                print(e)


        break
