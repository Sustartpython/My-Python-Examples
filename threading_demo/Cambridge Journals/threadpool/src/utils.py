import sqlite3
import mysql.connector
import pypyodbc
from PIL import Image
import io
import sys
import time
import os
import threading
import traceback
import json
import requests
from pyhdfs import HdfsClient
from Const import GolobalConfig
import base64
import hashlib


def BaseEncodeID(strRaw):
    r""" 自定义base编码 """

    strEncode = base64.b32encode(strRaw.encode('utf8')).decode('utf8')

    if strEncode.endswith('======'):
        strEncode = '%s%s' % (strEncode[0:-6], '0')
    elif strEncode.endswith('===='):
        strEncode = '%s%s' % (strEncode[0:-4], '1')
    elif strEncode.endswith('==='):
        strEncode = '%s%s' % (strEncode[0:-3], '8')
    elif strEncode.endswith('='):
        strEncode = '%s%s' % (strEncode[0:-1], '9')

    table = str.maketrans('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210')
    strEncode = strEncode.translate(table)

    return strEncode


def BaseDecodeID(strEncode):
    r""" 自定义base解码 """

    table = str.maketrans('ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210', '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    strEncode = strEncode.translate(table)

    if strEncode.endswith('0'):
        strEncode = '%s%s' % (strEncode[0:-1], '======')
    elif strEncode.endswith('1'):
        strEncode = '%s%s' % (strEncode[0:-1], '====')
    elif strEncode.endswith('8'):
        strEncode = '%s%s' % (strEncode[0:-1], '===')
    elif strEncode.endswith('9'):
        strEncode = '%s%s' % (strEncode[0:-1], '=')

    strRaw = base64.b32decode(strEncode.encode('utf8')).decode('utf8')

    return strRaw


def GetLngid(sub_db_id, rawid, case_insensitive=False):
    r""" 由 sub_db_id 和 rawid 得到 lngid。
    case_insensitive 标识源网站的 rawid 是否区分大小写
    """
    uppercase_rawid = ''  # 大写版 rawid
    if case_insensitive:  # 源网站的 rawid 区分大小写
        for ch in rawid:
            if ch.upper() == ch:
                uppercase_rawid += ch
            else:
                uppercase_rawid += ch.upper() + '_'
    else:
        uppercase_rawid = rawid.upper()

    limited_id = uppercase_rawid  # 限长ID
    if len(uppercase_rawid) > 20:
        limited_id = hashlib.md5(uppercase_rawid.encode('utf8')).hexdigest().upper()
    else:
        limited_id = BaseEncodeID(uppercase_rawid)

    lngid = sub_db_id + limited_id

    return lngid


def init_db(type_, database=None, databasenum=1):
    """
    初始化数据库连接，返回连接实例
    Arguments:
        type_ {string} -- mysql,sqlite3,mdb 中的一个
    
    Keyword Arguments:
        database {string} -- 指定数据库，如果是sqlite3,mdb则是具体的文件 (default: {None})
    
    Returns:
        sql.connect -- 数据库连接实例
    """

    conn = None
    if type_ == 'mysql':
        user = GolobalConfig['mysql']['DBUser']
        password = GolobalConfig['mysql']['DBPwd']
        if databasenum == 1:
            host = GolobalConfig['mysql']['DBHost1']
        elif databasenum == 2:
            host = GolobalConfig['mysql']['DBHost2']
        elif databasenum == 3:
            host = GolobalConfig['mysql']['DBHost3']
        elif databasenum == 4:
            host = GolobalConfig['mysql']['DBHost4']
        elif databasenum == 5:
            host = GolobalConfig['mysql']['DBHost5']
        else:
            host = GolobalConfig['mysql']['DBHost1']
        port = GolobalConfig['mysql']['DBPort']
        if database is None:
            raise ValueError('当指定的数据容器为`mysql`的时候，必须给定参数`database`的值')
        conn = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)

    elif type_ == 'sqlite3':
        if database is None:
            raise ValueError('当指定的数据容器为`sqlite3`的时候，必须给定参数`database`的值')
        conn = sqlite3.connect(database)

    elif type_ == 'mdb':
        if database is None:
            raise ValueError('当指定的数据容器为`mdb`的时候，必须给定参数`database`的值')
        connStr = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq={};'.format(database)
        conn = pypyodbc.connect(connStr)
    return conn


def Img2Jpg(buf, dstFile):
    exMsg = ''
    try:
        srcImg = Image.open(io.BytesIO(buf))
        dstImg = srcImg.resize((108, 150), Image.ANTIALIAS).convert('RGB')
        dstImg.save(dstFile, 'JPEG')
    except:
        exMsg = '* ' + traceback.format_exc()
        print(exMsg)
    if exMsg:
        return False

    return True


def parse_results_to_sql(conn, stmt, results, size=1):
    """
    批量执行SQL语句且提交到数据库

    Arguments:
        conn {sql.connect} -- 数据库连接实例
        stmt {string} -- 需要执行的SQL语句
        results {[(val,[val])]} -- 元素为元组的数组
    
    Keyword Arguments:
        size {integer} -- 当 results 为多大的时候执行 (default: {1})
    
    Raises:
        e -- SQL异常
    
    Returns:
        bool -- results有没有成功保存到数据库，成功返回True，失败返回False
    """

    sign = False
    if len(results) >= size:
        try:
            cur = conn.cursor()
            cur.executemany(stmt, results)
            conn.commit()
            cur.close()
        except Exception as e:
            raise e
        sign = True
    return sign


def printf(*args):
    print(time.strftime("%Y/%m/%d %X") + ' [info]', *args)


lock = threading.Lock()


def logerror(line):
    global lock
    cur_dir_fullpath = os.path.dirname(os.path.abspath(__file__))
    logpath = os.path.abspath(os.path.join(cur_dir_fullpath, r"..\log"))
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    fname = logpath + '/' + time.strftime("%Y%m%d") + '.txt'
    lock.acquire()
    try:
        with open(fname, mode='a', encoding='utf8') as f:
            f.write(line + '\n')
    except Exception as e:
        raise e
    finally:
        lock.release()


def file_list(filepath):
    """
    文件夹的遍历
    Arguments:
        filepath {string} -- 需要遍历的文件夹
    Yields:
        string,string -- 返回文件名跟文件绝对目录
    """

    for root, dirs, files in os.walk(filepath):
        for file in files:
            yield file, os.path.join(root, file)


def msg2weixin(msg):
    Headers = {
        'Accept':
            '*/*',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    }

    corpid = r'wwa7df1454d730c823'
    corpsecret = r'dDAusBg3gK7hKhLfqIRlyp84UDtII6NkMW7s8Wn2wgs'
    url = r'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s' % (corpid, corpsecret)
    count = 0
    while count < 3:
        try:
            r = requests.get(url)
            content = r.content.decode('utf8')
            dic = json.loads(content)
            accessToken = dic['access_token']

            usr = GolobalConfig['weixin']['User']
            url = r'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s' % accessToken
            form = {"touser": usr, "msgtype": "text", "agentid": 1000015, "text": {"content": msg}, "safe": 0}

            r = requests.post(url=url, data=json.dumps(form), headers=Headers, timeout=30)

            break
        except:
            count += 1
            printf('发送消息到企业微信失败')


def all_2_one(src, dst, size=2):
    """
    合并单个文件到一个大文件中，每个文件大小限制为2GB
    """
    import random
    new_dirname = time.strftime("%Y%m%d")
    new_file = dst + '/' + new_dirname + '_' + repr(random.randrange(111, 999)) + ".big_json"
    count = 0
    for _, files in file_list(src):
        with open(files, mode='r', encoding="utf-8") as fp:
            text = fp.readline()
            while text:
                with open(new_file, mode='a', encoding="utf-8") as f:
                    f.write(text)
                if os.path.getsize(new_file) // (1024 * 1024 * 1024) >= size:
                    new_file = dst + '/' + new_dirname + '_' + repr(random.randrange(111, 999)) + ".big_json"
                count += 1
                text = fp.readline()
    print(count)


def ProcOne(client, srcFile, dstFile):
    print('ProcOne \n%s\n -> \n%s ' % (srcFile, dstFile))

    #目标文件已经存在且大小相同
    if client.exists(dstFile) and \
    (os.path.getsize(srcFile) == client.list_status(dstFile)[0].length):
        print('file exists: %s ' % dstFile)
        return True

    #注意，如果已存在会被覆盖
    client.copy_from_local(srcFile, dstFile, overwrite=True)

    if os.path.getsize(srcFile) == client.list_status(dstFile)[0].length:  #校验文件大小
        return True

    return False


def ProcAll(LocalDir, HdfsDir):
    NameNode = GolobalConfig['hdfs']['NameNode']
    UserName = GolobalConfig['hdfs']['UserName']
    client = HdfsClient(hosts=NameNode, user_name=UserName)
    if not client.exists(HdfsDir):
        client.mkdirs(HdfsDir)
    total = len(os.listdir(LocalDir))
    processed = 0
    failedList = list()
    FileSize = 0
    StartTime = time.time()
    for filename in os.listdir(LocalDir):
        srcFile = os.path.join(LocalDir, filename)
        dstFile = HdfsDir + '/' + filename
        if not ProcOne(client, srcFile, dstFile):
            failedList.append(srcFile)
        else:
            FileSize += os.path.getsize(srcFile)
        processed += 1
        print('%d/%d/%d, time cost: %.2f s' % (total, processed, len(failedList), time.time() - StartTime))
        print('%d B, %.2f MB/s \n' % (FileSize, FileSize / 1024 / 1024 / (time.time() - StartTime)))

    if failedList:
        print('failedList: %s' % repr(failedList))
        return False
    else:
        print('Good! No Error!')
        print('%d B, %.2f MB, %.2f GB, %.2f MB/s' % \
          (FileSize, FileSize/1024/1024, FileSize/1024/1024/1024, FileSize/1024/1024/(time.time()-StartTime)))
        return True
