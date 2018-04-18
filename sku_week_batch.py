# -*- coding: utf-8 -*-
import pymysql.cursors

_host = 'ssnctrdb.crsiigb6itcn.ap-northeast-2.rds.amazonaws.com'
_user = 'sscnt'
_password = 'Fashion2017'
_db = 'bigdata'
conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')

print("db connect success")

# =====================================[[[[ DASH_BOARD - 대시보드 테이블 ]]]]=======================================================================
curs = conn.cursor()
sql = """
INSERT INTO bigdata_platform.DASH_BOARD
(DIVISION , ITEM_CLASS_CODE, SKU_ID, YEAR, WEEK_NUMBER, SALES_CNT,
	       SKU_NAME,SKU_CODE, TAG_PRICE, SALES_START_DATE,
	       GOODS_ID, GOODS_CODE, GOODS_NAME,COLLECT_SITE_CODE,
	      
	 		 STD_BRAND_CODE, BRAND_NAME,S3_PATH, COLOR_NAME, SIZE, MATERIAL_NAME)
         """
curs.execute(sql)
conn.commit()

conn.close()







