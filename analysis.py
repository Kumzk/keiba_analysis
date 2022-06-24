import sys
import pprint
pprint.pprint(sys.path)
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import pymysql

connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='password', db='racedata')
connection.autocommit(False)

load_dotenv()

# connection parameters
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')
url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

engine = sqlalchemy.create_engine(url)


def test():
    sql = '''
      SELECT  horse.father, count(horse.father)
      FROM race
      INNER JOIN result ON race.race_id = result.race_id
      INNER JOIN horse ON result.horse_id = horse.`id`
      WHERE place_id = 9
            AND race.length = 2200
              AND race.date_and_time > '2010-01-01 09:50:00'
              AND race.days <= 4
          AND race.turf_cond = "良"
          AND race.terms = "定量"
          AND result.`arrival_order` <= 3
          -- AND race.race_rank = 'オープン'
      GROUP BY horse.father;
    '''
    df = pd.read_sql(
    sql = sql,
    con = engine,
    )
    return df

if __name__ == '__main__':
  print("処理スタート")
  test = test()
  print(test)