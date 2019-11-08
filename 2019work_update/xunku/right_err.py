import requests

with open('err.txt', mode='r') as fp:
    line = fp.readline()
    while line:
        linelist = line.split('\t')
        Id = linelist[1]
        times = linelist[2]
        data = {
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
            'scriptSessionId':'z8sBNXHMKmfv3suE$pG7clNs7Pm/riRqbPm-qD2G8wqG5',
        }
        import re,json
        import pymysql,datetime
        conn = pymysql.connect('192.168.30.199','root','vipdatacenter','nantong')
        cur = conn.cursor()

        #查询报纸名字
        sql = "select name from paper_name where id = %s" %(Id)
        cur.execute(sql)
        names = cur.fetchall()
        for i in names:
            name = i[0]
        url = 'http://app.xunku.org/dwr/call/plaincall/EpaperSearch.getEdtions.dwr'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Rerfer':'http://app.xunku.org',
            'Cookie':'JSESSIONID=79D0F5FBC04EE3D487BBCFA05FBCE235; DWRSESSIONID=z8sBNXHMKmfv3suE$pG7clNs7Pm'
        }

        res = requests.post(url=url,headers=headers,data=data)
        pattern = re.compile(r'r.handleCallback\("40","0",(.*)\);')
        newpapaer_banmian =  pattern.findall(res.text)[0]
        paper_json = newpapaer_banmian.replace("anchor:","\"anchor\":").replace("id:","\"id\":")

        paper_jsons = json.loads(paper_json)
        for paper_info in paper_jsons:
            anchor = paper_info['anchor'].replace('\\','')
            Id = paper_info['id']
            ontime_id = str('%s')%times + '@@' + str(Id)
            sql = "insert ignore into paper_banmian (paper_name,anchor,ontime_id,id,on_time) values ('%s','%s','%s','%s','%s')" % (name,anchor,ontime_id,Id,times)
            print(sql)
            cur.execute(sql)
            conn.commit()
        line = fp.readline()

