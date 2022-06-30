import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import pymysql

load_dotenv()

user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')

# pymysqlの接続設定
connection = pymysql.connect(
                host=str(host),
                port=int(port),
                user=str(user),
                password=str(password),
                db=str(database),
                cursorclass=pymysql.cursors.DictCursor
              )
connection.autocommit(False)

# pandasのmysql接続設定
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')
url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
engine = sqlalchemy.create_engine(url)

def frame_no(place_id, length, days, turf_cond):
  """
  枠順毎の成績取得
  Param
  place_id: 
  """
  days = 'AND ra.days <= 4'
  with connection.cursor() as cursor:
    stmt = (f'''WITH count_tmp AS(
      SELECT
        re.frame_no as frame_no,
        re.arrival_order as arrival_order,
          count(re.seq) as re_count
      FROM race ra
      INNER JOIN result re ON ra.race_id = re.race_id
      WHERE ra.place_id = %s
          AND ra.length = %s
          AND ra.date_and_time > '2010-01-01 09:50:00'
          {days}
          AND ra.turf_cond = %s
          -- AND ra.race_rank = 'オープン'
      GROUP BY
        re.frame_no,
        re.arrival_order
      ORDER BY re.frame_no
      )
      SELECT
          frame_no as "枠番",
          no1 as '1着',
          no2 as "2着",
          no3 as '3着',
          no4 as '4着以下',
          concat( FORMAT(no1 / (no1 + no2 + no3 + no4) * 100, 0), "％") as "勝率",
          concat( FORMAT((no1 + no2 ) / (no1 + no2 + no3 + no4)  * 100, 0), "％") as "連対率",
          concat( FORMAT( (no1 + no2 + no3) / (no1 + no2 + no3 + no4)  * 100, 0), "％")as "3着以内"
      FROM(
          SELECT
              frame_no,
              MAX(CASE arrival_order WHEN 1 THEN re_count ELSE 0 END) as "no1",
              MAX(CASE arrival_order WHEN 2 THEN re_count ELSE 0 END) as "no2",
              MAX(CASE arrival_order WHEN 3 THEN re_count ELSE 0 END) as "no3",
              MAX(CASE WHEN arrival_order > 3 THEN re_count ELSE 0 END) as "no4"
          FROM
              count_tmp
          GROUP BY
              frame_no
      ) AS frame_arrival_order
    ''', [place_id, length, turf_cond])
    print(*stmt)
    cursor.execute(*stmt)
    results = cursor.fetchall()
    dic = dict()
    for n, item in enumerate(results):
      dic[n] = item

    return dic
    # df = pd.read_sql(sql = stmt, con = engine)
    # return df
    # return df.to_dict(orient='dict')

if __name__ == '__main__':
  print("処理スタート")
  test = frame_no(9, 2000, "<= 4", '良')
  print(test)