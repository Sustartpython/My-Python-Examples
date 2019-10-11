import toml
import pymysql

DBHOST = toml.load('config.toml')['DBHost']
DBPORT = toml.load('config.toml')['DBPort']
DBUSER = toml.load('config.toml')['DBUser']
DBPWD = toml.load('config.toml')['DBPwd']
DB = toml.load('config.toml')['DB']

conn = pymysql.connect(DBHOST, DBUSER, DBPWD, DB)

def read_sql(sql):
	r"""
	查询语句
	"""
	cur = conn.cursor()
	cur.execute(sql)
	sql_data = cur.fetchall()
	return sql_data

def update_sql(sql):
	r"""
	执行提交语句
	"""
	cur = conn.cursor()
	cur.execute(sql)
	conn.commit()

sql = "select url from lists where stat = 0"
results = read_sql(sql)
print(results)

sql_two = "insert into list(url,name) values ('www.baidu.com','百度')"
update_sql(sql)