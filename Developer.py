import math
import pyodbc
import requests
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime

from sqlalchemy import exc
from sqlalchemy.sql import text
from sqlalchemy import create_engine

## 取得系統現在時間
def get_datetime():
    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return time

def connect_sql(sql_db=None):  
    # if sql_db is None:
    #     sql_db = r'Origin'
    server = r""
    database = r""
    username = r""
    password = r""
    engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+':1433/'+database+'?driver=SQL+Server+Native+Client+11.0')
    return engine

def query2sql(sql):
    engine = connect_sql()
    engine.execute(text(sql).execution_options(autocommit=True))
    # engine.execute(sql)
    engine.dispose()
