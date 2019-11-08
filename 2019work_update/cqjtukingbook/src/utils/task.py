"""
this is an package
"""
import os
import configparser
import sys
from .const import *
import time
import utils

cf = configparser.ConfigParser()
cf.read(SRC_PATH + '/setting/config.ini')


class Task(object):

    def __init__(self):
        """初始化各个下载任务的目录"""

        # self.provider = ''  # 每个项目的 Provider
        # self.proxy = ''  #每个项目所需要的代理IP(针对特定采集源)
        try:
            if self.provider:
                self.project_name = self.provider
        except AttributeError as e:
            self.project_name = sys.argv[0][:-3]
        else:
            self.project_name = sys.argv[0][:-3]
        self.cf = cf
        update_path = "update"
        project_filepath = os.path.join(FILE_DOWNLOAD_PATH, self.project_name)
        self.index_path = os.path.join(project_filepath, 'index', update_path)
        self.list_path = os.path.join(project_filepath, 'list', update_path)
        self.detail_path = os.path.join(project_filepath, 'detail', update_path)
        self.cover_path = os.path.join(project_filepath, 'cover', update_path)
        self.html_path = os.path.join(project_filepath, 'html', update_path)
        self.data_path = os.path.join(FILE_DOWNLOAD_PATH, 'products')  # 成品目录

        self.template_file = os.path.join(
            self.data_path,
            self.project_name + '_' + update_path + '.db3',
        )

    def notify(self, dst, func, msg):
        """
        通知,将一些消息送至目的地(dst)
        """
        pass

    def init_db(self, type_):
        return utils.init_db(type_, self.provider)


class Download(Task):
    """
    下载类
    """

    def down_list(self):
        """下载列表页"""
        if not os.path.exists(self.list_path):
            os.makedirs(self.list_path)

    def down_index(self):
        """下载索引页"""
        if not os.path.exists(self.index_path):
            os.makedirs(self.index_path)

    def down_detail(self):
        """下载详情页"""
        if not os.path.exists(self.detail_path):
            os.makedirs(self.detail_path)

    def down_cover(self):
        """下载封面"""
        if not os.path.exists(self.cover_path):
            os.makedirs(self.cover_path)

    def down_html(self):
        """下载起始页"""
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)


class Parse(Task):
    """
    解析类
    """

    def parse_html(self):
        """解析起始页"""
        if not os.path.exists(self.html_path):
            raise FileNotFoundError('You must download the html_page(起始页) first.')

    def parse_list(self):
        """解析列表页"""
        if not os.path.exists(self.list_path):
            raise FileNotFoundError('You must download the list_page(列表页) first.')

    def parse_index(self):
        """解析索引页"""
        if not os.path.exists(self.index_path):
            raise FileNotFoundError('You must download the index_page(索引页) first.')

    def parse_detail(self):
        """解析详情页"""
        if not os.path.exists(self.detail_path):
            raise FileNotFoundError('You must download the detail_page(详情页) first.')
        if not os.path.exists(self.template_file):
            import shutil
            shutil.copy(self.cf.get("path", "template_path"), self.template_file)

    def clear(self):
        """将update文件夹更改为当天的时间(ymd(年月日))
        """
        new_dirname = time.strftime("%Y%m%d")
        dir_list = [
            self.index_path,
            self.list_path,
            self.html_path,
            self.detail_path,
            self.cover_path,
        ]
        for dir_ in dir_list:
            if os.path.exists(dir_):
                os.rename(dir_, os.path.dirname(dir_) + '/' + new_dirname)
