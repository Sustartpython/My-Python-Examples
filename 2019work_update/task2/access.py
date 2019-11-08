import pypyodbc
import toml

DBPATH = toml.load('config.toml')['Dbpath']
db = pypyodbc.win_connect_mdb(DBPATH)

curser = db.cursor()
sql = "select count(*) from books"
result = curser.execute(sql).fetchone()
print(result[0])

# if not exists (select id from books1 where id = 1 ) INSERT INTO books1 (bookname,url,years) VALUES('A','B','C') else update books1 set ,bookname='1' ,url='2312',years='123123' where id = 1