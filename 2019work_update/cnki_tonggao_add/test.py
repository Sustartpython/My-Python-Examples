import requests
import os
import json
import time
import pypyodbc

htmlpath = 'E:\work\cnki_add\html'
dbpath = 'E:\work\cnki_add\cnkitougao_info_20191107.mdb'

def down_html():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        'Accept':'*/*',
        'Cookie': 'Ecp_ClientId=8190929092300780432; cnkiUserKey=373b72fc-67a1-4ae2-ea8a-d11154394731; UM_distinctid=16e1b2611d66a6-0e42ad2acfca08-c343162-15f900-16e1b2611d767d; Ecp_IpLoginFail=19110658.17.245.128; DID=b94b690b-ce1d-408a-a933-fd86385108e4; RsPerPage=20; SID=013022; ASP.NET_SessionId=hyzslvmzum2gk5ngiviku52c; Hm_lvt_7ad0e218cfd89a6bcabf4ce749d7c3db=1573022104,1573095623; LID=WEEvREcwSlJHSldTTEYzU3EydDVHbUZZcXJYNTZvS085ZHZLV1doTTkxND0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!; c_m_expire=2019-11-8 11:15:27; c_m_LinID=LinID=WEEvREcwSlJHSldTTEYzU3EydDVHbUZZcXJYNTZvS085ZHZLV1doTTkxND0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!&ot=2019-11-8 11:15:27; Ecp_LoginStuts={"UserName":"17815175628","ShowName":"17815175628","IsAutoLogin":false,"UserType":"jf","r":"8116e2"}; Hm_lpvt_7ad0e218cfd89a6bcabf4ce749d7c3db=1573096533',
        'Connection': 'keep-alive',
        'Host': 'x.cnki.net',
    }
    n_list = ['A','B','C','D','E','F','G','H','I','J']
    for num in n_list:
        url = 'http://x.cnki.net/psmc/Rss/GetPublicationWithContribution?time=1573096626388&fldCode=%s&pageIndex=1&pageSize=10000' % num
        res = requests.get(url,headers=headers)
        fname = htmlpath + '/' + num +'.html'
        with open(fname,'w',encoding='utf-8')as f:
            f.write(res.text)
        print('down %s right' % num)

def parse_html():
    db = pypyodbc.win_connect_mdb(dbpath)
    for roots, dirs, files in os.walk(htmlpath):
        for file in files:
            fpath = roots + '/' + file
            with open(fpath,encoding='utf-8')as f:
                text = f.read()
            html = json.loads(text)
            r_list = html['Data']['Sucess']
            for dicts in r_list:
                journal_id = dicts['PYKM']
                journal_name = dicts['C_NAME']
                CBPT = dicts['Url2']
                if CBPT == None:
                    CBPT = ''
                url = dicts['Url1']
                if url == None:
                    url = ''
                issn = dicts['ISSN']
                cn = dicts['CN']
                sql = """insert into info(journal_id,journal_name,CBPT,官网,issn,cn) values(?,?,?,?,?,?)""" 
                message = (journal_id, journal_name, CBPT, url, issn, cn)
                curser = db.cursor()
                curser.execute(sql,message)
                curser.commit()
                print('%s插入成功' % journal_name)

if __name__ == "__main__":
    # down_htm()
    parse_html()