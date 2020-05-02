from utils import fullpath 
import os 
import pyodbc 
import pandas as pd 


DRIVER = os.environ.get('DRIVER')
SERVER = os.environ.get('SERVER')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
DATABASE = os.environ.get('DATABASE')

dsn = 'driver=%s;server=%s;uid=%s;pwd=%s' % (
  DRIVER, 
  SERVER, 
  USERNAME, 
  '' if PASSWORD is None else PASSWORD, 
)

def daiei_connect():
  try:
    return pyodbc.connect(dsn) 
  except Exception as e:
    return e 

def daiei_query(sql, **kargs):
  with daiei_connect() as conn:
    return pd.read_sql(sql, conn, coerce_float=False, **kargs).fillna('').astype(str)