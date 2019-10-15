import pypyodbc

# 数据库路径
DBPATH = r'E:\work\ADD\eshukan\data\eshukan.mdb'
# 链接数据库
db = pypyodbc.win_connect_mdb(DBPATH)

# 和mysql一样的增删改查
sql = "insert into xxxxxx"
curser = db.cursor()
curser.execute(sql)
curser.commit()
