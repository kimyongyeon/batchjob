# -*- coding: utf-8 -*-
import pymysql.cursors
import os
from datetime import datetime

import telegram
# 토큰을 지정해서 bot을 선언해 줍시다! (물론 이 토큰은 dummy!)i, @c_log_bot 친구 추가 
bot = telegram.Bot(token='465314041:AAHDuX7EDCmIx1CQvaOhSek9MyQ8S0OJaZY')
# 우선 테스트 봇이니까 가장 마지막으로 bot에게 말을 건 사람의 id를 지정해줄게요.
# 만약 IndexError 에러가 난다면 봇에게 메시지를 아무거나 보내고 다시 테스트해보세요.
chat_id = bot.getUpdates()[-1].message.chat.id
bot.sendMessage(chat_id=chat_id, text='크롤링 시작')

_host = 'ssnctrdb.crsiigb6itcn.ap-northeast-2.rds.amazonaws.com'
_user = 'sscnt'
_password = 'Fashion2017'
_db = 'crawler_op'
conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')


# DB 조회
def db_list(_query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(_query)
            return cursor.fetchall()

    finally:
        conn.close()


# 오늘 일자 구하기.
def def_today():
    month = datetime.today().month
    day = datetime.today().day
    year = datetime.today().year
    if (len(str(month)) == 1):
        month = '0' + str(month)
    if (len(str(day)) == 1):
        day = '0' + str(day)
    return str(year) + "-" + str(month) + "-" + str(day)


query = '''
select GOODS_IMAGE from MSCNT_SKU_INFO where  date_format(REG_DT, '%Y-%m-%d') = '{0}' ORDER BY  REG_DT ASC
'''.format(
    def_today())
db_file_list = db_list(query)
# print("DB FILE LIST:" + str(db_file_list))
file_db_new_list = []
folder_db_new_set = set()
for db_file in db_file_list:
    db_file = list(db_file)[0]
    file_db_new_list.append(db_file.split('/')[2])
    folder_db_new_set.add(db_file.split('/')[1])

folder_db_new_list = list(folder_db_new_set)


# DB 조회
def db_list(_query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(_query)
            return cursor.fetchall()

    finally:
        conn.close()


# 파일 읽기
def def_file_read(file_path):
    f = open(file_path, 'r', encoding='utf8')
    line = f.readline()
    return line
    f.close()


# 파일 존재 여부 체크
resultlist = []


log_file_name = 'crawl_log_' + def_today() + '.log'
os.remove(log_file_name)

def def_file_search_check(main_path, file_db_new_list):
    try:
        file_list = os.listdir(main_path)
        # print("DB FILE LIST:" + str(file_db_new_list))
        # print("REAL FILE LIST:" + str(file_list))
        # print ("path:" + main_path)
        print("file_db_new_list len:" + str(len(file_db_new_list)))
        print("file_list len:" + str(len(file_list)))

        for db_file in file_db_new_list:
            for real_file in file_list:
                # print ("real-file:" + real_file)
                if real_file in db_file:
                    resultlist.append(db_file)
        log_file_list = set(resultlist) - set(file_db_new_list)
        f = open(log_file_name, 'a')
        log = main_path + "\nDB-LEN:" + str(len(file_db_new_list)) + "\nREAL-LEN:" + str(
            len(resultlist)) + "\nlog_file_list: " + str(log_file_list)
        print(log)
        bot.sendMessage(chat_id=chat_id, text=log)
        f.write(log)
        f.close()

    except Exception as e:
        print(" 파일을 쓰기중 에러 발생! " + str(e))


# DARKVICTORY 처리
def def_sub_file(forder):
    try:
        if "DARKVICTORY" in forder:
            main_path = '/app/ec/imageserver/public/crawler/' + forder + "/DARKVIC"
            def_file_search_check(main_path, file_db_new_list)

        else:  # GMARKET
            main_path = '/app/ec/imageserver/public/crawler/' + forder + "/"
            sub_list = ['AIN', 'IMVELY', 'STYLENANDA']
            for sub in sub_list:
                main_path = main_path + sub
                def_file_search_check(main_path, file_db_new_list)
    except:
        print(main_path + " 폴더가 존재 하지 않습니다.")


# ===============================================================
# 파일 찾는다.
# ===============================================================
# forder_list = def_file_read('file.txt').split(';')
for forder in folder_db_new_list:
    #   print(forder)
    main_path = '/app/ec/imageserver/public/crawler/' + forder + "/"
    if "DARKVICTORY" in forder:
        def_sub_file(forder)
        continue
    if "GMARKET" in forder:
        def_sub_file(forder)
        continue

    try:
        def_file_search_check(main_path, file_db_new_list)

    except:
        print(main_path + " 폴더가 존재 하지 않습니다.")

bot.sendMessage(chat_id=chat_id, text='크롤링 종료')
