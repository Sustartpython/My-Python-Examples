"""
公共模块包，所有项目适用。

"""
import io
import os
import sqlite3
import sys
import time
# from os import PathLike

import mysql.connector
import pyhdfs
import pypyodbc
import requests
from PIL import Image
import configparser
from .task import Download, Parse, Task, cf
from .const import *


def _get_proxy():
    """读取采集到的代理IP"""
    with open(UTILS_PATH + '/IP.txt', mode='r', encoding='utf8') as f:
        proxies = [proxy.strip() for proxy in f.readlines()]
    return proxies


def _proxy_custom(queue):
    """一个代理生产队列"""
    proxies = _get_proxy()
    for proxy in proxies:
        while True:
            if queue.full():
                continue
            else:
                queue.put(proxy)
                break


def proxy_thread(queue, task_queue):
    """当代理队列中的IP数量小于等于 1 的时候，
    就重新从数据库里面读取加入到队列当中当中
    """
    _proxy_custom(queue)
    while True:
        if task_queue.empty():
            break
        if queue.qsize() <= 1:
            _proxy_custom(queue)


def init_db(type_, database=None):
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
        config_file = SRC_PATH + '/setting/config.ini'
        if not os.path.exists(config_file):
            raise FileNotFoundError('配置文件 {} 不存在'.format(config_file))
        user = cf.get('mysql', 'user')
        password = cf.get('mysql', 'password')
        # database = cf.get('mysql', 'database')
        host = cf.get('mysql', 'host')
        port = cf.get('mysql', 'port')
        if database is None:
            database = sys.argv[0][:-3],
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


def upload_file_to_hdfs(hdfs_path, src_path):
    """
    上传文件到HDFS文件系统，如果文件夹不存在就创建
    
    Arguments:
        hdfs_path {string} -- HDFS文件夹
        src_path {string} -- 源地址文件夹
    
    Raises:
        ValueError -- 当`hdfs_path`不是类似`Linux`文件路径就会抛出
        e -- `pyhdfs`模块的异常
    """

    if not hdfs_path.startswith("/"):
        raise ValueError('属性`hdfs_path`必须为有效的hadoop平台路径，例如 /RawData/chaoxing/duxiu_ts/big_htm')
    name_node = cf.get('hadoop', 'namenode')
    try:
        client = pyhdfs.HdfsClient(hosts=name_node)
        if not client.exists(hdfs_path):  # 如果hadoop平台文件夹不存在，就创建文件夹
            client.mkdirs(hdfs_path)
        for _, local_file in file_list(src_path):
            filename = os.path.basename(local_file)
            dst_file = hdfs_path + '/' + filename
            client.copy_from_local(local_file, dst_file, overwrite=True)
    except pyhdfs.HdfsException as e:
        raise e


def printf(*args):
    print(time.strftime("%Y/%m/%d %X") + ' [info]', *args)


def _get_http_respanse(method, url, feature, kind=None, **kwargs):
    """
    返回 HTTP Response
    :param type_: requests or requests.Session
    :param method: post or get
    :return: requetst.Response or None
    """
    if method not in ['post', 'get']:
        raise ValueError("http request must be a right method like post or get")
    try:
        if kind is None:
            if method == "post":
                resp = requests.post(url, headers=HEADER, **kwargs)
            else:
                resp = requests.get(url, headers=HEADER, **kwargs)
        else:
            if not isinstance(kind, requests.Session):
                raise ValueError("session's value must be isinstance of requests.Session")
            if method == "post":
                resp = kind.post(url, headers=HEADER, **kwargs)
            else:
                resp = kind.get(url, headers=HEADER, **kwargs)
    except requests.ConnectionError as e:
        printf(e, "Connection Error")
        return None
    except requests.Timeout as e:
        printf(e, "ReadTime out")
        return None
    if resp.status_code != 200:
        printf('the status_code is', resp.status_code, "not 200")
        return None
    if feature:
        if resp.content.decode('utf8').find(feature) == -1:
            printf(url, "返回的页面没有包含特征值 {}".format(feature))
            return None
    return resp


# get_html = _get_http_respanse()


def get_html(url, feature=None, timeout=20, **kwargs):
    """
    用GET请求来获取HTTP响应
    
    Arguments:
        url {string} -- 要请求的URL
    
    Keyword Arguments:
        feature {string} -- 正确网页的特征码 (default: {None})
        timeout {integer} -- 连接超时最大时间 (default: {20})
    
    Returns:
        requests.Response -- HTTP响应
    """

    return _get_http_respanse('get', url=url, feature=feature, timeout=timeout, **kwargs)


def get_html_by_post(url, feature=None, timeout=20, **kwargs):
    """用 post 请求获取 http 请求 返回 response"""
    return _get_http_respanse('post', url=url, feature=feature, timeout=timeout, **kwargs)


def get_html_by_session(session, url, feature=None, timeout=20, **kwargs):
    """用 session 的方式请求获取 http 请求 返回 response"""
    return _get_http_respanse('get', url, kind=session, feature=feature, timeout=timeout, **kwargs)


def all_2_one(src, dst, size=2):
    """
    合并单个文件到一个大文件中，每个文件大小限制为2GB
    """
    import random
    # Raw_data = PATH + "/book_detail/raw_data/"
    prefix = sys.argv[0][:-3]
    new_file = dst + '/' + prefix + repr(random.randrange(111, 999)) + ".big_htm"
    count = 0
    for roots, files in file_list(src):
        with open(files, mode='r', encoding="utf-8") as fp:
            text = fp.readline()
        text = text.replace("\t", " ")
        with open(new_file, mode='a', encoding="utf-8") as f:
            f.write(text + '\n')
        if os.path.getsize(new_file) // (1024 * 1024 * 1024) >= size:
            # print(os.path.getsize(fileName))
            new_file = dst + '/' + prefix + repr(random.randrange(111, 999)) + ".big_htm"
        count += 1
        if count % 10000 == 0:
            printf(count)
    printf(count)


def hadoop_shell(shell):
    os.chdir(UTILS_PATH + '/hadoop')
    os.system(shell)


def is_valid_img(file, binary=False):
    '''
    判断文件是否为有效（完整）的图片,输入参数为文件路径，或文件对象，或者为二进制
    '''
    bValid = True
    if not binary:
        if isinstance(file, str):
            fileObj = open(file, 'rb')
        else:
            fileObj = file

        buf = fileObj.read()
        if buf[6:10] in (b'JFIF', b'Exif'):  #jpg图片
            if not buf.rstrip(b'\0\r\n').endswith(b'\xff\xd9'):
                bValid = False
        else:
            try:
                Image.open(fileObj).verify()
            except:
                bValid = False
    else:
        if file[6:10] in (b'JFIF', b'Exif'):  #jpg图片
            if not file.rstrip(b'\0\r\n').endswith(b'\xff\xd9'):
                bValid = False
        else:
            try:
                Image.open(io.BytesIO(file)).verify()
            except:
                bValid = False

    return bValid