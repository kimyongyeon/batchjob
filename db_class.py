#-*- coding: utf-8 -*-
import pymysql.cursors

class DbManagement(object):

    def __init__(self, _host, _user, _password, _db):
        self._conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')
        if self._conn != None:
            print ("connected")
        else :
            print ("disconnected")

    # database create
    def create_database(self, _db_name):
        try:
            with self._conn.cursor() as cursor:
                sql = 'CREATE DATABASE {db_name}'.format(db_name=_db_name)
                cursor.execute(sql)
            self._conn.commit()
        finally:
            self._conn.close()

    # table create
    def create_table(self, _query):
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_query)
            self._conn.commit()
        finally:
            self._conn.close()

    # insert, update, delete
    def query_commit(self, _query, _val):
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_query, _val)
            self._conn.commit()
            print(cursor.rowcount)  # 1 (affected rows)
        finally:
            self._conn.close()

    # cursor
    def query_cursor_proc(self, _query, _val, _sel):
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_query, _val)
                if _sel == 1:
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()

                cols = cursor.description

                dic = {
                    'col': cols,
                    'result': result
                }
                return dic
        finally:
            self._conn.close()

host = 'ssnctrdb.crsiigb6itcn.ap-northeast-2.rds.amazonaws.com'
user = 'sscnt'
password = 'Fashion2017'
db = 'bigdata_platform'
db_mng = DbManagement(host, user, password, db)

query = '''
SELECT * FROM CHANNEL 
''' # 쿼리
val = () # 파라미터

result = db_mng.query_cursor_proc(query, val, 2)
print (result['col']) # 컬럼 전체
print (result['result']) # 결과

print ("len: " + str(len(result)))
print ("col: " + str(len(result['col'])))
print ("result: " + str(len(result['result'])))

list = []
for i in result['col']:
    dic = {
        i[0]: '1'
    }
    list.append(dic)
print (str(list))

for row in result['result']:
    for col in row:
        print ("str: "  + str(col))








