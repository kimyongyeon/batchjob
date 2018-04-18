# -*- coding: utf-8 -*-
# #####################################################################################
# 제목 : SSF TO bigdata 데이터 동기화
# 주요테이블 : BRAND, COLOR, GOODS_COLOR, GOODS, SKU, MATERIAL, SKU_IMG, SIZE, SKU_SIZE
# 개발일자 : 18.04.17
# 목적 : SSF DB를 bigdata DB로 비즈니스 로직에 맞게 이관하기 위함.
# #####################################################################################

##########################################################
# mySql package info
##########################################################
import pymysql.cursors

##########################################################
# mySql connection info
##########################################################
_host = 'ssnctrdb.crsiigb6itcn.ap-northeast-2.rds.amazonaws.com'
_user = 'sscnt'
_password = 'Fashion2017'
_db = 'bigdata'

##########################################################
# SELECT 공통
##########################################################
def getList(sql):
    conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()

##########################################################
# INSERT 공통
##########################################################
def insertQuery(sql):
    conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()

##########################################################
# UPDATE 공통
##########################################################
def updateQuery(sql):
    # cursor.lastrowid
    conn = pymysql.connect(host=_host, user=_user, password=_password, db=_db, charset='utf8mb4')
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
    finally:
        conn.close()


##########################################################
# SSF.SKU_BASE 전체조회
##########################################################
def findSkuList():
    sql = '''
        SELECT 
            SKU_CODE
          , GOODS_NAME
          , COLOR
          , BRAND_NAME
          , SSFCATEGORY
          , TAG_PRICE
          , ORIGIN
          , DESCR
          , COLLECT_PATH
          , SALES_START_DATE
          , SALES_END_DATE
          , PRDLST_CD
         FROM SSF.SKU_BASE LIMIT 0, 10
    '''
    return getList(sql)

##########################################################
# BIGDATA.SKU 키값 조회
##########################################################
def findSkuInfo(skuCode):
    sql= '''
    SELECT SKU_ID as skuId
		FROM bigdata.SKU S INNER JOIN bigdata.GOODS G ON S.GOODS_ID=G.GOODS_ID
		WHERE SKU_CODE='{0}'
		AND OWN_YN='Y'
    '''.format(skuCode)
    return getList(sql)

##########################################################
# SSF.SKU_IMAGE 전체조회
##########################################################
def findSsfSkuImageList(skuImgInfo):
    sql= '''
        SELECT SKU_CODE, IMAGE_PATH FROM SSF.SKU_IMAGE WHERE SKU_CODE='{0}'
    '''.format(skuImgInfo['SKU_CODE'])
    return getList(sql)

##########################################################
# BIGDATA.SKU_IMG 조회
##########################################################
def findSkuImg(skuImgInfo):
    sql='''
        SELECT
			SKU_IMG_ID
		FROM bigdata.SKU_IMG
		WHERE SKU_ID='{0}' AND S3_PATH='{1}'
	'''.format(skuImgInfo['SKU_ID'],skuImgInfo['S3_PATH'] )
    return getList(sql)

def findSkuImgRepYnCheck(skuImgInfo):
    sql='''
        SELECT
			COUNT(*) CNT
		FROM bigdata.SKU_IMG
		WHERE SKU_ID='{0}' AND S3_PATH='{1}' AND REP_YN = 'Y'
	'''.format(skuImgInfo['SKU_ID'],skuImgInfo['S3_PATH'] )
    return getList(sql)

##########################################################
# BIGDATA.SKU_IMG 저장
##########################################################
def insSkuImg(insSkuImgDic):
    sql='''
        INSERT INTO bigdata.SKU_IMG
		(
			  SKU_ID
			, ORIGIN_PATH
			, THUMBNAIL_PATH
			, VECTOR_YN
			, REG_DATE_TIME
			, REG_USER
		    , REP_YN
		    , S3_PATH
		    , ORIGIN_DIV
		)
		VALUES
		(
			  '{0}'
			, '{1}'
			, '{2}'
			, '{3}'
			, '{4}'
			, '{5}'
		    , '{6}'
		    , '{7}'
		    , '{8}'
		)
    '''.format(
        insSkuImgDic['SKU_ID']
        ,insSkuImgDic['ORIGIN_PATH']
        ,insSkuImgDic['THUMBNAIL_PATH']
        ,insSkuImgDic['VECTOR_YN']
        ,insSkuImgDic['REG_DATE_TIME']
        ,insSkuImgDic['REG_USER']
        ,insSkuImgDic['REP_YN']
        ,insSkuImgDic['S3_PATH']
        ,insSkuImgDic['ORIGIN_DIV']
    )
    return insertQuery(sql)

##########################################################
# BIGDATA.SKU_IMG 업데이트
##########################################################
def upSkuImg(upSkuImgDic):
    sql = '''
    UPDATE bigdata.SKU_IMG SET
		      ORIGIN_PATH='{0}'
			, UPDATE_DATE_TIME='{1}'
			, UPDATE_USER='{2}'
			, REP_YN='{3}'
			, S3_PATH='{4}'
		    , ORIGIN_DIV='{5}'
		    , UPDATE_DATE_TIME=NOW()
		    , UPDATE_USER='REST_API_ADMIN'
		WHERE SKU_IMG_ID='{6}'
    '''.format(
        upSkuImgDic['ORIGIN_PATH']
        ,upSkuImgDic['UPDATE_DATE_TIME']
        ,upSkuImgDic['UPDATE_USER']
        ,upSkuImgDic['REP_YN']
        ,upSkuImgDic['S3_PATH']
        ,upSkuImgDic['ORIGIN_DIV']
        ,upSkuImgDic['SKU_IMG_ID'])
    print(sql)
    updateQuery(sql)

if __name__ == "__main__":
    skuInfoList = findSkuList()
    skuSuccessCnt = 0
    for skuInfo in skuInfoList:
        skuCode = skuInfo['SKU_CODE']
        print ("SKU_CODE:" + skuCode)
        selSkuInfo = findSkuInfo(skuCode)
        skuInfoLen = len(selSkuInfo)
        if skuInfoLen > 0:
            skuId = selSkuInfo[0]['skuId']
            print("SKU_ID:" + str(skuId))
            ##################################################################
            # SKU_IMG
            ##################################################################
            ssfSkuImgDic = {}
            ssfSkuImgDic['SKU_CODE'] = skuCode
            # ---------------------------------------------------
            # SSF 조회
            # ---------------------------------------------------
            ssfSkuImgList = findSsfSkuImageList(ssfSkuImgDic)
            cnt = 0
            for ssfSkuImgInfo in ssfSkuImgList:

                bigdataSkuImgDic = {}
                bigdataSkuImgDic['SKU_ID'] = skuId
                bigdataSkuImgDic['S3_PATH'] = ssfSkuImgInfo['IMAGE_PATH']
                # ---------------------------------------------------
                # bigdata 조회
                # ---------------------------------------------------
                bSkuImgList = findSkuImg(bigdataSkuImgDic)
                bSkuImgLength = len(bSkuImgList)
                bSkuImgRepYnList = findSkuImgRepYnCheck(bigdataSkuImgDic)
                repCheck = int(bSkuImgRepYnList[0]['CNT'])

                if bSkuImgLength == 0:
                    insSkuImgDic = {}
                    insSkuImgDic['SKU_ID'] = skuId
                    insSkuImgDic['ORIGIN_PATH'] = ssfSkuImgInfo['IMAGE_PATH']
                    insSkuImgDic['THUMBNAIL_PATH'] = ''
                    insSkuImgDic['VECTOR_YN'] = 'N'
                    insSkuImgDic['REG_DATE_TIME'] = 'NOW()'
                    insSkuImgDic['REG_USER'] = 'REST_API_ADMIN'

                    if cnt == 0:
                        insSkuImgDic['REP_YN'] = 'Y'
                        cnt = cnt + 1
                    else:
                        insSkuImgDic['REP_YN'] = 'N'

                    insSkuImgDic['S3_PATH'] = ssfSkuImgInfo['IMAGE_PATH']
                    insSkuImgDic['ORIGIN_DIV'] = 'SSF_INF'

                    # insert SKU_IMG
                    insSkuImg(insSkuImgDic)
                    skuSuccessCnt = skuSuccessCnt + 1
                else:
                    if (repCheck == 0):
                        for skuImg in bSkuImgList:
                            upSkuImgDic = {}
                            upSkuImgDic['SKU_IMG_ID'] = skuImg['SKU_IMG_ID']
                            upSkuImgDic['SKU_ID'] = skuId
                            upSkuImgDic['ORIGIN_PATH'] = ssfSkuImgInfo['IMAGE_PATH']
                            upSkuImgDic['THUMBNAIL_PATH'] = ''
                            upSkuImgDic['VECTOR_YN'] = 'N'
                            upSkuImgDic['UPDATE_DATE_TIME'] = 'NOW()'
                            upSkuImgDic['UPDATE_USER'] = 'REST_API_ADMIN'

                            if cnt == 0:
                                upSkuImgDic['REP_YN'] = 'Y'
                                cnt = cnt + 1
                            else:
                                upSkuImgDic['REP_YN'] = 'N'

                            upSkuImgDic['S3_PATH'] = ssfSkuImgInfo['IMAGE_PATH']
                            upSkuImgDic['ORIGIN_DIV'] = 'SSF_INF'

                            # update SKU_IMG
                            upSkuImg(upSkuImgDic)

                            skuSuccessCnt = skuSuccessCnt + 1

    print("success count: " + str(skuSuccessCnt))
