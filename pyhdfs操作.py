import pyhdfs

#链接hdfs
client = pyhdfs.HdfsClient(hosts='hadoop2x-01:50070,hadoop2x-02:50070',user_name='suh')

#返回这个用户的根目录
print(client.get_home_directory())

#返回指定目录下的所有文件
# print(client.listdir('/RawData/apsjournal/big_json/2019/20190729'))


# 从本地上传文件至集群之前，集群的目录
print('Before !%s' % client.listdir('/RawData/apsjournal/big_json/2019/20190729'))

#从本地上传文件至集群
# client.copy_from_local('E:/work/Meeting/aiaa/big_json/suhtest.txt','/RawData/apsjournal/big_json/2019/20190729/s.txt')

# 从本地上传文件至集群之后，集群的目录
# print('After !%s' % client.listdir('/RawData/apsjournal/big_json/2019/20190729'))

# 先看看文件中的内容
response = client.open("/RawData/apsjournal/big_json/2019/20190729/20190729_191.big_json")
response.read()

# 创建新目录
# client.mkdirs()

# 查看文件是否存在
client.exists("/user/hadoop/test.csv")  #Fales不存在,True存在

