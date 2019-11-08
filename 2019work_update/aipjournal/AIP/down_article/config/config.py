#encoding: utf-8
#author: walker
#date: 
#summary:

import os, sys
from configparser import ConfigParser

cur_dir_fullpath  =  os.path.dirname(os.path.abspath(__file__))

class Config(object):
    def __init__(self):
        cfg = ConfigParser()
        cfgFile = os.path.abspath(os.path.join(cur_dir_fullpath, "..\\..\\config\\config.ini"))  
        if not os.path.exists(cfgFile):
            input(cfgFile + ' not found')
            sys.exit(-1)
        cfgLst = cfg.read(cfgFile, encoding='utf8')
        if len(cfgLst) < 1:
            input('Read config.ini failed...')
            sys.exit(-1)

        self.DBHost = cfg.get('db', 'DBHost').strip()	
        self.DBPort = int(cfg.get('db', 'DBPort').strip())
        self.DBUser = cfg.get('db', 'DBUser').strip()
        self.DBPwd = cfg.get('db', 'DBPwd').strip()
        self.DBName = cfg.get('db', 'DBName').strip()
        self.ArticleTable = cfg.get('db', 'ArticleTable').strip()
        self.ProxyTable = cfg.get('db', 'ProxyTable').strip() 
        
        self.MQHost = cfg.get('mq', 'Host').strip()
        self.MQVirtualHost = cfg.get('mq', 'VirtualHost').strip()
        self.MQUser = cfg.get('mq', 'User').strip()
        self.MQPwd = cfg.get('mq', 'Pwd').strip()
        
        self.MQQueueCenter2Node = cfg.get('get_article', 'MQQueueCenter2Node').strip()
        self.MQQueueNode2Center = cfg.get('get_article', 'MQQueueNode2Center').strip()

        self.ArticleRoot = cfg.get('get_article', 'ArticleRoot').strip()

        self.WorkerThreadNumber = int(cfg.get('get_article', 'WorkerThreadNumber').strip())

        print('DBHost: %s' % self.DBHost)
        print('DBPort: %d' % self.DBPort)
        print('DBUser: %s' % self.DBUser)
        print('DBPwd: %s' % self.DBPwd)
        print('DBName: %s' % self.DBName)
        print('ArticleTable: %s' % self.ArticleTable)
        print('ProxyTable: %s' % self.ProxyTable)
        print('MQHost: %s' % self.MQHost)
        print('MQVirtualHost: %s' % self.MQVirtualHost)
        print('MQUser: %s' % self.MQUser)
        print('MQPwd: %s' % self.MQPwd)
        
        print('MQQueueCenter2Node: %s' % self.MQQueueCenter2Node)
        print('MQQueueNode2Center: %s' % self.MQQueueNode2Center)

        print('ArticleRoot: %s' % self.ArticleRoot)

        print('WorkerThreadNumber: %d' % self.WorkerThreadNumber)
            
        print('Read config.ini completed!')

if __name__ == '__main__':
    pass
    #ReadConfig()
    #PrintConfig()