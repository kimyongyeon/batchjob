# -*- coding: utf-8 -*-
import pymysql.cursors

_host = 'ssnctrdb.crsiigb6itcn.ap-northeast-2.rds.amazonaws.com'
_user = 'sscnt'
_password = 'Fashion2017'
_db = 'bigdata'
conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')

# ====================================[[[ RETAIL_BRAND - 리테일별 브랜드 ]]]]==================================
curs = conn.cursor()
sql = """
DELETE FROM bigdata_platform.RETAIL_BRAND;
INSERT INTO bigdata_platform.RETAIL_BRAND(RETAIL_DIV_CODE,RETAIL_DIV_NAME, RETAIL_CODE,RETAIL_NAME,STD_BRAND_CODE,STD_BRAND_NAME)
SELECT DISTINCT
		C.COMMON_CODE AS RETAIL_DIV_CODE , 
      C.CODE_NAME AS RETAIL_DIV_NAME,
		B.SITE_CODE AS RETAIL_CODE,
		B.SITE_NAME AS RETAIL_NAME,
		A.STD_BRAND_CODE, A.STD_BRAND_NAME
from bigdata.VIEW_GOODS A
INNER JOIN bigdata.COLLECT_SITE B ON A.COLLECT_SITE_CODE=B.SITE_CODE
INNER JOIN bigdata.COMMON_CODE C ON B.DIVISION_CODE=C.COMMON_CODE; 
         """
print ("db insert count: ", curs.execute(sql))
conn.commit()

# ====================================[[[ DAILY_SKU_STAT - 일자별 SKU 수(필터용) ]]] ============================================
curs = conn.cursor()
sql = """
DELETE FROM bigdata_platform.DAILY_SKU_STAT;
INSERT INTO bigdata_platform.DAILY_SKU_STAT (
     	  SALES_START_DATE,
        COLLECT_SITE_CODE, 
        ITEM_CLASS_CODE,  
        TYPE_CODE, ITEM_CODE, GROUP_CODE,
       	STD_CODE, BRAND_NAME,
        SKU_CNT
)
	SELECT  A.SALES_START_DATE,
			A.COLLECT_SITE_CODE, 
			A.ITEM_CLASS_CODE,  
			A.TYPE_CODE, A.ITEM_CODE, A.GROUP_CODE,
			A.STD_BRAND_CODE, A.STD_BRAND_NAME,
			 COUNT(*) 
	FROM bigdata.VIEW_GOODS A
	WHERE A.COLLECT_SITE_CODE <>'OWN'
	AND A.SALES_START_DATE IS NOT NULL
	GROUP BY  A.SALES_START_DATE,
			A.COLLECT_SITE_CODE, 
			A.ITEM_CLASS_CODE,  
			A.TYPE_CODE, A.ITEM_CODE, A.GROUP_CODE,
			A.STD_BRAND_CODE, A.STD_BRAND_NAME;
         """
print ("db insert count: ", curs.execute(sql))
conn.commit()


conn.close()







