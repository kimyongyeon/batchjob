# -*- coding: utf-8 -*-
# #####################################################################################
# 제목 : SSF TO bigdata 데이터 동기화
# 주요테이블 : SKU, SKU_IMG
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
# SSF.SKU_MATERIAL 전체조회
##########################################################
def findSsfMaterialList(materialInfo):
    sql= '''
        SELECT SKU_CODE, MATERIAL_DIVISION, MATERIAL_NAME, RATE FROM SSF.SKU_MATERIAL
        WHERE SKU_CODE='{0}' 
    '''.format(materialInfo['SKU_CODE'])
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
# SSF.SKU_SIZE 전체조회
##########################################################
def findSkuSizeList(sizeInfo):
    sql = '''
            SELECT SKU_CODE, SIZE FROM SSF.SKU_SIZE WHERE SKU_CODE='{0}'
        '''.format(sizeInfo['SKU_CODE'])
    return getList(sql)

##########################################################
# BIGDATA.GOODS_MATERIAL 키값 조회
##########################################################
def findGoodMaterialInfo(selMaterialInfo):
    sql = '''
        SELECT MATERIAL_ID as materialId
		FROM bigdata.GOODS_MATERIAL
		WHERE GOODS_ID = '{0}' 
		AND MATERIAL_NAME= '{1}' 
		limit 0, 1
    '''.format(selMaterialInfo['GOODS_ID'], selMaterialInfo['MATERIAL_NAME'])
    return getList(sql)

##########################################################
# BIGDATA.GOODS_MATERIAL 저장
##########################################################
def insMaterial(insMaterialInfo):
    sql='''
    INSERT INTO bigdata.GOODS_MATERIAL
		(
			  GOODS_ID
			, MATERIAL_DIVISION
			, MATERIAL_NAME
			, RATE
			, REG_DATE_TIME
			, REG_USER
		)
		VALUES
		(
			  '{0}'
			, '{1}'
			, '{2}'
			, '{3}'
			, '{4}'
			, '{5}'
		)
	'''.format(
        insMaterialInfo['GOODS_ID']
        ,insMaterialInfo['MATERIAL_DIVISION']
        ,insMaterialInfo['MATERIAL_NAME']
        ,insMaterialInfo['RATE']
        ,insMaterialInfo['REG_DATE_TIME']
        ,insMaterialInfo['REG_USER']
    )
    print (sql)
    insertQuery(sql)

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

##########################################################
# BIGDATA.SIZE 조회
##########################################################
def findSize(skuBigdataSizeInfo):
    sql='''
        SELECT SIZE_ID AS sizeId
        FROM bigdata.SIZE
        WHERE SIZE_NAME = '{0}' 
    '''.format(skuBigdataSizeInfo['SIZE_NAME'])
    return getList(sql)

##########################################################
# BIGDATA.SIZE 저장
##########################################################
def insSize(insSizeInfo):
    sql='''
        INSERT INTO bigdata.SIZE
		(
			  SIZE_NAME
			, REG_DATE_TIME
			, REG_USER
		)
		VALUES
		(
			  '{0}'
			, '{1}'
			, '{2}'
		)
    '''.format(
        insSizeInfo['SIZE_NAME']
        ,insSizeInfo['REG_DATE_TIME']
        ,insSizeInfo['REG_USER']
    )

##########################################################
# BIGDATA.SKU_SIZE 저장
##########################################################
def insSkuSize(insSkuSizeInfo):
    sql = '''
        INSERT INTO bigdata.SKU_SIZE
		(
			  SKU_ID
			, SIZE_ID
			, REG_DATE_TIME
			, REG_USER
		)
		VALUES
		(
			  '{0}'
			, '{1}'
			, '{2}'
			, '{3}'
		)
    '''.format(
        insSkuSizeInfo['SKU_ID']
        ,insSkuSizeInfo['SIZE_ID']
        ,insSkuSizeInfo['REG_DATE_TIME']
        ,insSkuSizeInfo['REG_USER']
    )
    return insertQuery(sql)

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
         FROM SSF.SKU_BASE
         WHERE SKU_CODE IN (
             '1143270021'
         )
         LIMIT 0, 10
    '''
    return getList(sql)

##########################################################
# BIGDATA.COLOR 조회
##########################################################
def findColorList(colorName):
    sql = '''
        SELECT COLOR_NAME, COLOR_ID FROM bigdata.COLOR WHERE COLOR_NAME='{0}'
    '''.format(colorName)
    return getList(sql)

##########################################################
# BIGDATA.BRAND 조회
##########################################################
def findBrandList(brandName):
    sql = '''
        SELECT BRAND_NAME, BRAND_ID FROM bigdata.BRAND WHERE BRAND_NAME='{0}'
    '''.format(brandName)
    return getList(sql)

##########################################################
# BIGDATA.STD_CATEGORY 조회
##########################################################
def findCategoryList(categoryCode):
    sql = '''
        SELECT
          UPPER_CODE as upperCode,
          CATEGORY_NAME as categoryName,
          CATEGORY_CODE as categoryCode,
          ITEM_CLASS_CODE as itemClassCode
        FROM bigdata.STD_CATEGORY
        WHERE CATEGORY_CODE = '{0}'
    '''.format(categoryCode)
    return getList(sql)

##########################################################
# BIGDATA.GOODS 키값 조회
##########################################################
def findGoodsList(goodsCode):
    sql = '''
        SELECT
            GOODS_ID as goodsId, COLLECT_SITE_CODE as collectSiteCode
        FROM
            bigdata.GOODS
        WHERE
            GOODS_CODE='{0}' AND OWN_YN='Y'
    '''.format(goodsCode)
    return getList(sql)

##########################################################
# BIGDATA.GOODS 저장
##########################################################
def insGoods(insSkuInfo):
    sql = '''
        INSERT INTO bigdata.GOODS
        (
              BRAND_ID
            , TYPE_CODE
            , ITEM_CLASS_CODE
            , GROUP_CODE
            , ITEM_CODE
            , ORIGIN
            , TAG_PRICE
            , DESCR
            , STD_CATEGORY_CODE
            , GOODS_CODE
            , COLLECT_SITE_CODE
            , OWN_YN
            , ORIGIN_DIV
            , ITEM_CLASS_CODE_VIEW
            , TYPE_CODE_VIEW
            , GROUP_CODE_VIEW
            , ITEM_CODE_VIEW
            , REG_DATE_TIME
            , REG_USER
            , SALES_START_DATE
            , SALES_END_DATE
            , GOODS_NAME
            , COLLECT_PATH
        )
        VALUES (
              '{0}'
            , '{1}'
            , '{2}'
            , '{3}'
            , '{4}'
            , '{5}'
            , '{6}'
            , '{7}'
            , '{8}'
            , '{9}'
            , '{10}'
            , '{11}'
            , '{12}'
            , '{13}'
            , '{14}'
            , '{15}'
            , '{16}'
            , '{17}'
            , '{18}'
            , '{19}'
            , '{20}'
            , '{21}'
            , '{22}'
        )
    '''.format(
          insSkuInfo['BRAND_ID']
        , insSkuInfo['TYPE_CODE']
        , insSkuInfo['ITEM_CLASS_CODE']
        , insSkuInfo['GROUP_CODE']
        , insSkuInfo['ITEM_CODE']
        , insSkuInfo['ORIGIN']
        , insSkuInfo['TAG_PRICE']
        , insSkuInfo['DESCR']
        , insSkuInfo['STD_CATEGORY_CODE']
        , insSkuInfo['GOODS_CODE']
        , insSkuInfo['COLLECT_SITE_CODE']
        , insSkuInfo['OWN_YN']
        , insSkuInfo['ORIGIN_DIV']
        , insSkuInfo['ITEM_CLASS_CODE_VIEW']
        , insSkuInfo['TYPE_CODE_VIEW']
        , insSkuInfo['GROUP_CODE_VIEW']
        , insSkuInfo['ITEM_CODE_VIEW']
        , insSkuInfo['REG_DATE_TIME']
        , insSkuInfo['REG_USER']
        , insSkuInfo['SALES_START_DATE']
        , insSkuInfo['SALES_END_DATE']
        , insSkuInfo['GOODS_NAME']
        , insSkuInfo['COLLECT_PATH']
    )
    print (sql)
    return insertQuery(sql)

##########################################################
# BIGDATA.GOODS_COLOR 조회
##########################################################
def insGoodsColor(insGoodsColorInfo):
    sql = '''
        INSERT INTO bigdata.GOODS_COLOR
        (
              GOODS_ID
            , COLOR_ID
        )
        VALUES
        (
             '{0}'
            ,'{1}'
        )
    '''.format(insGoodsColorInfo['GOODS_ID'], insGoodsColorInfo['COLOR_ID'])
    return insertQuery(sql)

##########################################################
# BIGDATA.GOODS 업데이트
##########################################################
def updateGoods(upSkuInfo):
    sql = '''
        UPDATE bigdata.GOODS SET
              BRAND_ID = '{0}'
            , ORIGIN = '{1}'
            , TAG_PRICE = '{2}'
            , DESCR = '{3}'
            , STD_CATEGORY_CODE = '{4}'
            , ITEM_CLASS_CODE = '{5}'
            , ITEM_CLASS_CODE_VIEW = '{6}'
            , UPDATE_DATE_TIME = now()
            , UPDATE_USER = 'REST_API_ADMIN'
            , SALES_START_DATE = '{7}'
            , SALES_END_DATE = '{8}'
            , GOODS_NAME = '{9}'
            , COLLECT_PATH = '{10}'
            , ORIGIN_DIV = 'SSF_INF'
            , GOODS_CODE = '{11}'
        WHERE GOODS_ID='{12}'
        AND OWN_YN='Y'
    '''.format(
         upSkuInfo['BRAND_ID']
        ,upSkuInfo['ORIGIN']
        ,upSkuInfo['TAG_PRICE']
        ,upSkuInfo['DESR']
        ,upSkuInfo['STD_CATEGORY_CODE']
        ,upSkuInfo['ITEM_CLASS_CODE']
        ,upSkuInfo['ITEM_CLASS_CODE_VIEW']
        ,upSkuInfo['SALES_START_DATE']
        ,upSkuInfo['SALES_END_DATE']
        ,upSkuInfo['GOODS_NAME']
        ,upSkuInfo['COLLECT_PATH']
        ,upSkuInfo['GOODS_CODE']
        ,upSkuInfo['GOODS_ID']
    )
    print(sql)
    updateQuery(sql)

##########################################################
# BIGDATA.SKU 저장
##########################################################
def insSku(insSkuInfo):
    sql = '''
        INSERT INTO bigdata.SKU
        (
              SKU_CODE
            , GOODS_ID
            , COLOR_ID
            , SKU_NAME
            , ORIGIN
            , TAG_PRICE
            , DESCR
            , SALES_START_DATE
            , SALES_END_DATE
            , ORIGIN_DIV
            , REG_DATE_TIME
            , REG_USER
            , COLLECT_SITE_CODE
            , COLLECT_PATH
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
            , '{9}'
            , '{10}'
            , '{11}'
            , '{12}'
            , '{13}'
        )
    '''.format(
          insSkuInfo['SKU_CODE']
        , insSkuInfo['GOODS_ID']
        , insSkuInfo['COLOR_ID']
        , insSkuInfo['SKU_NAME']
        , insSkuInfo['ORIGIN']
        , insSkuInfo['TAG_PRICE']
        , insSkuInfo['DESCR']
        , insSkuInfo['SALES_START_DATE']
        , insSkuInfo['SALES_END_DATE']
        , insSkuInfo['ORIGIN_DIV']
        , insSkuInfo['REG_DATE_TIME']
        , insSkuInfo['REG_USER']
        , insSkuInfo['COLLECT_SITE_CODE']
        , insSkuInfo['COLLECT_PATH']
    )
    print(sql)
    return insertQuery(sql)

##########################################################
# BIGDATA.SKU 업데이트
##########################################################
def updateSku(upSkuInfo):
    sql = '''
      UPDATE bigdata.SKU SET
          COLOR_ID = '{0}'
        , SKU_NAME = '{1}'
        , ORIGIN = '{2}'
        , TAG_PRICE = '{3}'
        , DESCR = '{4}'
        , SALES_START_DATE = '{5}'
        , SALES_END_DATE = '{6}'
        , COLLECT_PATH = '{7}'
        , ORIGIN_DIV = '{8}'
        , UPDATE_DATE_TIME = '{9}'
        , UPDATE_USER = '{10}'
      WHERE SKU_ID='{11}'
    '''.format(
        upSkuInfo['COLOR_ID']
        ,upSkuInfo['SKU_NAME']
        ,upSkuInfo['ORIGIN']
        ,upSkuInfo['TAG_PRICE']
        ,upSkuInfo['DESCR']
        ,upSkuInfo['SALES_START_DATE']
        ,upSkuInfo['SALES_END_DATE']
        ,upSkuInfo['COLLECT_PATH']
        ,upSkuInfo['ORIGIN_DIV']
        ,upSkuInfo['UPDATE_DATE_TIME']
        ,upSkuInfo['UPDATE_USER']
        , upSkuInfo['SKU_ID']
    )
    print(sql)
    updateQuery(sql)

# =================================================================
#                            메인시작
# =================================================================
# 1. brandName으로 brandId 찾기
# 2. colorName으로 colorId 찾기
# 3. ssfCategory로 itemClassCode 찾기
# 4. GOODS,GOODS_COLOR upsert : insert시 GOODS_COLOR도 insert
# 5. SKU upsert
# 6. MATERIAL upsert
# 7. SKU_IMG upsert
# 8. SIZE, SKU_SIZE upsert
# =================================================================
if __name__ == "__main__":
    skuInfoList = findSkuList()
    for skuInfo in skuInfoList:
        print (skuInfo)
        skuCode = skuInfo['SKU_CODE']
        ##################################################################
        # 1. COLOR
        ##################################################################
        colorName = skuInfo['COLOR'] # colorId 찾기용
        colorId = findColorList(colorName) # colorId
        if colorId != ():
            colorId = colorId[0]["COLOR_ID"]
        else:
            colorId = 0
        ##################################################################
        # 2. BRAND
        ##################################################################
        brandName = skuInfo['BRAND_NAME'] # brandId 찾기용
        brandId = findBrandList(brandName) # brandId
        if brandId != ():
            brandId = brandId[0]["BRAND_ID"]
        else:
            brandId = 0
        ##################################################################
        # 3. ITEM_CLASS_CODE
        ##################################################################
        categoryCode = skuInfo['SSFCATEGORY'] # category 찾기용
        itemClassCode = findCategoryList(categoryCode) # itemClassCode
        if itemClassCode != ():
            itemClassCode = itemClassCode[0]['itemClassCode']
        else:
            itemClassCode = "WOMEN"

        ##################################################################
        # 4. GOODS
        ##################################################################
        goodsInfo = findGoodsList(skuCode)
        goodsId = ''
        goodsInfoLen = len(goodsInfo)
        if goodsInfoLen == 0:
            insSkuInfo = {}
            insSkuInfo['BRAND_ID'] = brandId
            insSkuInfo['TYPE_CODE'] = 'UNCLASSIFIED'
            insSkuInfo['ITEM_CLASS_CODE'] = itemClassCode
            insSkuInfo['GROUP_CODE'] = 'UNCLASSIFIED'
            insSkuInfo['ITEM_CODE'] = 'UNCLASSIFIED'
            insSkuInfo['ORIGIN'] = skuInfo['ORIGIN']
            insSkuInfo['TAG_PRICE'] = skuInfo['TAG_PRICE']
            insSkuInfo['DESCR'] = skuInfo['DESCR']
            insSkuInfo['STD_CATEGORY_CODE'] = skuInfo['SSFCATEGORY']
            insSkuInfo['GOODS_CODE'] = 'TEST'
            insSkuInfo['COLLECT_SITE_CODE'] = 'SSFSHOP'
            insSkuInfo['OWN_YN'] = 'Y'
            insSkuInfo['ORIGIN_DIV'] = 'SSF_INF'
            insSkuInfo['ITEM_CLASS_CODE_VIEW'] = itemClassCode
            insSkuInfo['TYPE_CODE_VIEW'] = 'UNCLASSIFIED'
            insSkuInfo['GROUP_CODE_VIEW'] = 'UNCLASSIFIED'
            insSkuInfo['ITEM_CODE_VIEW'] = 'UNCLASSIFIED'
            insSkuInfo['REG_DATE_TIME'] = 'NOW()'
            insSkuInfo['REG_USER'] = 'REST_API_ADMIN'
            insSkuInfo['SALES_START_DATE'] = skuInfo['SALES_START_DATE']
            insSkuInfo['SALES_END_DATE'] = skuInfo['SALES_END_DATE']
            insSkuInfo['GOODS_NAME'] = skuInfo['GOODS_NAME']
            insSkuInfo['COLLECT_PATH'] = skuInfo['COLLECT_PATH']

            # insert
            goodsId = insGoods(insSkuInfo)
            print(goodsId)

            insGoodsColorInfo = {}
            insGoodsColorInfo['GOODS_ID'] = goodsId
            insGoodsColorInfo['COLOR_ID'] = colorId
            insGoodsColor(insGoodsColorInfo)
        else:
            goodsId = goodsInfo[0]['goodsId']
            upSkuInfo = {}
            upSkuInfo['BRAND_ID'] = brandId
            upSkuInfo['ORIGIN'] = skuInfo['ORIGIN']
            upSkuInfo['TAG_PRICE'] = skuInfo['TAG_PRICE']
            upSkuInfo['DESR'] = skuInfo['DESCR']
            upSkuInfo['STD_CATEGORY_CODE'] = skuInfo['SSFCATEGORY']
            upSkuInfo['ITEM_CLASS_CODE'] = itemClassCode
            upSkuInfo['ITEM_CLASS_CODE_VIEW'] = itemClassCode
            upSkuInfo['UPDATE_DATE_TIME'] = 'NOW()'
            upSkuInfo['UPDATE_USER'] = 'REST_API_ADMIN'
            upSkuInfo['SALES_START_DATE'] = skuInfo['SALES_START_DATE']
            upSkuInfo['SALES_END_DATE'] = skuInfo['SALES_END_DATE']
            upSkuInfo['GOODS_NAME'] = skuInfo['GOODS_NAME']
            upSkuInfo['COLLECT_PATH'] = skuInfo['COLLECT_PATH']
            upSkuInfo['ORIGIN_DIV'] = 'SSF_INF'
            upSkuInfo['GOODS_CODE'] = skuCode
            upSkuInfo['GOODS_ID'] = goodsId

            # print (upSkuInfo)
            # update
            updateGoods(upSkuInfo)

        ##################################################################
        # SKU
        ##################################################################
        selSkuInfo = findSkuInfo(skuCode)
        skuId = ''
        skuInfoLen = len(selSkuInfo)
        if skuInfoLen == 0:
            insSkuInfo = {}
            insSkuInfo['SKU_CODE'] = skuCode
            insSkuInfo['GOODS_ID'] = goodsId
            insSkuInfo['COLOR_ID'] = colorId
            insSkuInfo['SKU_NAME'] = skuInfo['GOODS_NAME']
            insSkuInfo['ORIGIN'] = skuInfo['ORIGIN']
            insSkuInfo['TAG_PRICE'] = skuInfo['TAG_PRICE']
            insSkuInfo['DESCR'] = skuInfo['DESCR']
            insSkuInfo['SALES_START_DATE'] = skuInfo['SALES_START_DATE']
            insSkuInfo['SALES_END_DATE'] = skuInfo['SALES_END_DATE']
            insSkuInfo['ORIGIN_DIV'] = 'SSF_INF'
            insSkuInfo['REG_DATE_TIME'] = 'NOw()'
            insSkuInfo['REG_USER'] = 'REST_API_ADMIN'
            insSkuInfo['COLLECT_SITE_CODE'] = 'SSFSHOP'
            insSkuInfo['COLLECT_PATH'] = skuInfo['COLLECT_PATH']

            # insert
            skuId = insSku(insSkuInfo)
        else:
            skuId = selSkuInfo[0]['skuId']
            upSkuInfo = {}
            upSkuInfo['COLOR_ID'] = colorId
            upSkuInfo['SKU_NAME'] = skuInfo['GOODS_NAME']
            upSkuInfo['ORIGIN'] = skuInfo['ORIGIN']
            upSkuInfo['TAG_PRICE'] = skuInfo['TAG_PRICE']
            upSkuInfo['DESCR'] = skuInfo['DESCR']
            upSkuInfo['SALES_START_DATE'] = skuInfo['SALES_START_DATE']
            upSkuInfo['SALES_END_DATE'] = skuInfo['SALES_END_DATE']
            upSkuInfo['COLLECT_PATH'] = skuInfo['COLLECT_PATH']
            upSkuInfo['ORIGIN_DIV'] = 'SSF_INF'
            upSkuInfo['UPDATE_DATE_TIME'] = 'NOW()'
            upSkuInfo['UPDATE_USER'] = 'REST_API_ADMIN'
            upSkuInfo['SKU_ID'] = skuId

            # update
            updateSku(upSkuInfo)

        ##################################################################
        # MATERIAL
        ##################################################################
        ssfMaterialDic = {}
        ssfMaterialDic['SKU_CODE']=skuCode
        # ---------------------------------------------------
        # SSF 조회
        # ---------------------------------------------------
        ssfMaterialList = findSsfMaterialList(ssfMaterialDic)
        for ssfMaterialInfo in ssfMaterialList:
            bigdataMaterialDic = {}
            bigdataMaterialDic['GOODS_ID']=goodsId
            bigdataMaterialDic['MATERIAL_NAME']=ssfMaterialInfo['MATERIAL_NAME']
            # ---------------------------------------------------
            # bigdata 조회
            # ---------------------------------------------------
            bigDataMaterialList = findGoodMaterialInfo(bigdataMaterialDic)
            bigDataMaterialListLength = len(bigDataMaterialList)
            if bigDataMaterialListLength == 0:
                insMaterialInfo = {}
                insMaterialInfo['GOODS_ID']=goodsId
                insMaterialInfo['MATERIAL_DIVISION']=ssfMaterialInfo['MATERIAL_DIVISION']
                insMaterialInfo['MATERIAL_NAME']=ssfMaterialInfo['MATERIAL_NAME']
                insMaterialInfo['RATE']=ssfMaterialInfo['MATERIAL_NAME']
                insMaterialInfo['REG_DATE_TIME']='NOW()'
                insMaterialInfo['REG_USER']='REST_API_ADMIN'
                # insert GOODS_MATERIAL
                insMaterial(insMaterialInfo)

        ##################################################################
        # SKU_IMG
        ##################################################################
        ssfSkuImgDic = {}
        ssfSkuImgDic['SKU_CODE'] = skuCode
        # ---------------------------------------------------
        # SSF 조회
        # ---------------------------------------------------
        ssfSkuImgList = findSsfSkuImageList(ssfSkuImgDic)
        cnt=0
        for ssfSkuImgInfo in ssfSkuImgList:
            bigdataSkuImgDic = {}
            bigdataSkuImgDic['SKU_ID']=skuId
            bigdataSkuImgDic['S3_PATH']=ssfSkuImgInfo['IMAGE_PATH']
            # ---------------------------------------------------
            # bigdata 조회
            # ---------------------------------------------------
            bSkuImgList = findSkuImg(bigdataSkuImgDic)
            bSkuImgLength = len(bSkuImgList)
            bSkuImgRepYnList = findSkuImgRepYnCheck(bigdataSkuImgDic)
            repCheck = int(bSkuImgRepYnList[0]['CNT'])
            if bSkuImgLength == 0:
                insSkuImgDic = {}
                insSkuImgDic['SKU_ID']=skuId
                insSkuImgDic['ORIGIN_PATH']=ssfSkuImgInfo['IMAGE_PATH']
                insSkuImgDic['THUMBNAIL_PATH']=''
                insSkuImgDic['VECTOR_YN'] = 'N'
                insSkuImgDic['REG_DATE_TIME'] = 'NOW()'
                insSkuImgDic['REG_USER'] = 'REST_API_ADMIN'

                if cnt==0:
                    insSkuImgDic['REP_YN']='Y'
                    cnt = cnt + 1
                else:
                    insSkuImgDic['REP_YN']='N'

                insSkuImgDic['S3_PATH']=ssfSkuImgInfo['IMAGE_PATH']
                insSkuImgDic['ORIGIN_DIV'] = 'SSF_INF'

                # insert SKU_IMG
                insSkuImg(insSkuImgDic)
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

        ##################################################################
        # SIZE
        ##################################################################
        ssfSkuSizeDic = {}
        ssfSkuSizeDic['SKU_CODE'] = skuCode
        # ---------------------------------------------------
        # SSF 조회
        # ---------------------------------------------------
        ssfSizeList = findSkuSizeList(ssfSkuSizeDic)
        for ssfSize in ssfSizeList:
            skuBigdataSizeInfo = {}
            # skuBigdataSizeInfo[''] = ssfSize['SKU_CODE']
            skuBigdataSizeInfo['SIZE_NAME'] = ssfSize['SIZE']
            # ---------------------------------------------------
            # bigdata 조회
            # ---------------------------------------------------
            bSizeList = findSize(skuBigdataSizeInfo)
            bSizeListLen = len(bSizeList)
            if bSizeListLen == 0:
                insSizeInfo={}
                insSizeInfo['SIZE_NAME'] = ssfSize['SIZE']
                # insert SIZE
                sizeId = insSize(insSizeInfo)
                insSkuSizeInfo = {}
                insSkuSizeInfo['SKU_ID']=skuId
                insSkuSizeInfo['SIZE_ID']=sizeId
                insSkuSizeInfo['REG_DATE_TIME'] = 'NOW()'
                insSkuSizeInfo['REG_USER'] = 'REST_API_ADMIN'
                # insert SKU_SIZE
                insSkuSize(insSkuSizeInfo)