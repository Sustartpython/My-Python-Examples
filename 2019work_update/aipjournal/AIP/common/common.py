# encoding: utf-8
# author: walker
# date: 
# summary:

import os, sys
from collections import OrderedDict
from configparser import ConfigParser
from pprint import pprint

cur_dir_fullpath  =  os.path.dirname(os.path.abspath(__file__))


# 读取配置文件
# 返回一个二级字典
def ReadConfig():
    cfg = ConfigParser()
    cfg.optionxform = str   # 保持键的大小写
    cfgFile = os.path.abspath(os.path.join(cur_dir_fullpath, r"..\config\config.ini"))  
    if not os.path.exists(cfgFile):
        input(cfgFile + ' not found')
        sys.exit(-1)
    with open(cfgFile, mode='rb') as f:
        content = f.read()
    if content.startswith(b'\xef\xbb\xbf'):     # 去掉 utf8 bom 头
        content = content[3:]
    cfg.read_string(content.decode('utf8'))
    if not cfg.sections():
        input('Read config.ini failed...')
        sys.exit(-1)
    
    dic = OrderedDict()
    for section in cfg.sections():
        dic[section] = OrderedDict()
        for option in cfg.options(section):
            dic[section][option] = cfg.get(section, option).strip()

    pprint(dic) 
    print('Read %s completed!' % cfgFile)
    
    return dic

GolobalConfig = ReadConfig()

if __name__ == '__main__':
    pass