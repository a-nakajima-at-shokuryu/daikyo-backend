from utils import fullpath 
from daiei_db import daiei_query 
import csv 
from fastapi import APIRouter 

router = APIRouter()

@router.get('/')
async def index():
  df = dataframe()
  return df.to_dict(orient='record')

def dataframe():
  DATABASE = 'DAIEI_DB'
  def table(tablename): 
    return '%s.dbo.%s' % (DATABASE, tablename)

  GZAIKOZAN = table('GZAIKOZAN') # 在庫残高マスタ
  KURADRMST = table('KURADRMST') # 冷蔵庫住所マスタ
  TEKIYOMST = table('TEKIYOMST') # 摘要マスタ
  HINZNSMST = table('HINZNSMST') # 全社品種マスタ																																																HFUZUIMST															HFU										

  def TEKIYO_Subquery(TEKNM):
    return '\n'.join([
      '('
      'SELECT', 
      ','.join([
        'TEK_TEKNM',  # ｺｰﾄﾞ　摘要名
        'TEK_TEKCD',  # ｺｰﾄﾞ　摘要
        'TEK_TANSH',  # 短縮名　摘要
        'TEK_SEISI',  # 正式名　摘要
      ]), 
      'FROM %s' % TEKIYOMST, 
      'WHERE TEK_TEKNM = \'%s\'' % TEKNM, 
      ') AS %s' % TEKNM, 
    ])

  sql = '\n'.join([
    'SELECT', 
    ','.join([
      'GZA_HINCD', # コード　品種
      'HIZ_HINNM', # 品名
      'HIZ_SIZEN', # サイズ									
      'HIZ_YORYO', # 容量
      'HIZ_JURKB', # 区分　重量（Ｇ・Ｐ）
      'HIZ_IRISU', # 入数
      'GZA_HTANK', # 平均単価																			

      'GZA_ZAIKB', # 区分　在庫種別

      'GZA_ZSHNO', # 在庫処理NO
      'GZA_NYKNO', # 入庫NO（証券NO）
      'GZA_NSKKB', # 区分　入出庫 （# 棚No?）
      'GZA_NKYMD', # 日付　入庫年月日
      'GZA_UPYMD', # 日付　最終更新年月日																			

      'GZA_MOTOK', # 数量　元個数（枚数）
      'GZA_MKING', # 金額　元個数
      'GZA_GENZA', # 残数　現在庫（枚数）
      'GZA_JUCHU', # 確定前受注数（枚数）
      
      'KAD_COPRM', # （冷蔵庫）会社名　略式名称
      'KAD_FCTRM', # （冷蔵庫）工場名　略式名称

      'GZA_GENCD', # 原産地コード
      'GZA_KAKCD', # 加工地コード
      'GZA_IKUCD', # 育成コード
      'GZA_TAICD', # 態様コード
      'GZA_KURCD', # 倉コード

      'TGEN.TEK_TANSH as TGEN_GENNM', # （原産地）短縮名　摘要
      'TKAK.TEK_TANSH as TKAK_GENNM', # （加工地）短縮名　摘要
      'TIKU.TEK_TANSH as TIKU_IKUNM', # （育成）短縮名　摘要
      'TTAI.TEK_TANSH as TTAI_TAINM', # （様態）短縮名　摘要

    ]), 

    'FROM %s AS GZA' % GZAIKOZAN, 
    
    'LEFT OUTER JOIN %s AS HIZ' % HINZNSMST, 
    'ON GZA.GZA_HINCD = HIZ.HIZ_HINCD', 

    'LEFT OUTER JOIN %s AS KAD' % KURADRMST, 
    'ON GZA.GZA_KURCD = KAD.KAD_KURCD', 

    'LEFT OUTER JOIN %s' % TEKIYO_Subquery('TGEN'), 
    'ON GZA.GZA_GENCD = TGEN.TEK_TEKCD', 

    'LEFT OUTER JOIN %s' % TEKIYO_Subquery('TKAK'), 
    'ON GZA.GZA_KAKCD = TKAK.TEK_TEKCD', 

    'LEFT OUTER JOIN %s' % TEKIYO_Subquery('TIKU'), 
    'ON GZA.GZA_IKUCD = TIKU.TEK_TEKCD', 

    'LEFT OUTER JOIN %s' % TEKIYO_Subquery('TTAI'), 
    'ON GZA.GZA_TAICD = TTAI.TEK_TEKCD', 

    'WHERE', 
    '(', 
    'GZA_GENZA <> 0'
    'OR', 
    'GZA_NKYMD >= \'2020-02-01\'', 
    ')', 
    
    'ORDER BY', 
    ','.join([
      'GZA_HINCD',
      'GZA_KURCD', 
      'GZA_ZSHNO', 
      'GZA_NKYMD',
    ]),  
  ])

  df = daiei_query(sql)

  filename = fullpath('../csv', 'GZAIKOZAN.csv')
  df.to_csv(filename, quoting=csv.QUOTE_ALL)

  return df
