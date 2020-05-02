from utils import fullpath 
from daiei_db import daiei_query 
import csv 
from fastapi import APIRouter

router = APIRouter()

@router.get('/')
async def index():
  df = dataframe()
  return df.to_dict(orient='records')

def dataframe():
  DATABASE = 'DAIEI_DB'
  
  def table(tablename):
    return '%s.dbo.%s' % (DATABASE, tablename) 

  AITSAKMST = table('AITSAKMST')

  sql = '\n'.join([
    'SELECT', 
    ','.join([
      '*', 
    ]), 
    'FROM %s' % AITSAKMST, 
    'WHERE', 
    'AIT_USBKB = \'U\'', # 区分　売掛・買掛識別
    'ORDER BY', 
    ','.join([
      'AIT_AITCD', 
    ]), 
  ])

  df = daiei_query(sql)

  filename = fullpath('../csv', 'AITSAKMST_U.csv')
  df.to_csv(filename, quoting=csv.QUOTE_ALL)

  return df 

