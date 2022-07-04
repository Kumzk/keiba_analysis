import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import pymysql
import json


class Analysis():
  '''分析で使うベースクラス'''
  def __init__(self, place_id: int, is_turf: bool, length: int):
    """
    Parameters
    ----------
    place_id: int
      対象の競馬場 placeテーブルを参照
    is_turf: bool
      true: 芝レース, false: ダートレース
    length: int 
      レースの距離
    """
    self.place_id = place_id
    self.race_type = '芝' if is_turf else 'ダ'
    self.race_type_en = 'turf' if is_turf else 'dirt'
    self.length = length

    load_dotenv()
    user = os.environ.get('user')
    password = os.environ.get('password')
    host = os.environ.get('host')
    port = os.environ.get('port')
    database = os.environ.get('database')

    # pymysqlの接続設定
    self.pool = pymysql.connect(
                  host=str(host),
                  port=int(port),
                  user=str(user),
                  password=str(password),
                  db=str(database),
                  cursorclass=pymysql.cursors.DictCursor
                )
    self.pool.autocommit(False)

    # pandasのmysql接続設定
    user = os.environ.get('user')
    password = os.environ.get('password')
    host = os.environ.get('host')
    port = os.environ.get('port')
    database = os.environ.get('database')
    url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    self.pandas_pool = sqlalchemy.create_engine(url)
  
  def setTerms(self, turf_cond: str, days: int=3):
    self.days = days
    if days == 1: # 開催前半
      self.days_stmt = 'AND ra.days <= 2'
    if days == 2:
      self.days_stmt = 'AND ra.days >= 5'
    else:
      self.days_stmt = ''
    
    self.turf_cond = turf_cond
    if turf_cond == '良':
      self.turf_cond_en = 'good'
    elif turf_cond == '重':
      self.turf_cond_en = 'bad'

  def __base_stmt(self, column: str, column_str: str): #ベースのSQL
    stmt = f'''
        WITH count_tmp AS(
          SELECT
            re.{column} as {column},
            re.arrival_order as arrival_order,
            count(re.seq) as re_count
          FROM race ra
          INNER JOIN result re ON ra.race_id = re.race_id
          WHERE ra.place_id = {self.place_id}
              AND ra.length = {self.length}
              AND ra.race_type = '{self.race_type}'
              AND ra.date_and_time > '2010-01-01 09:50:00'
              AND ra.turf_cond = '{self.turf_cond}'
              {self.days_stmt}
              -- AND ra.race_rank = 'オープン'
          GROUP BY
            re.{column},
            re.arrival_order
          ORDER BY re.{column}
        )
        SELECT
            {column} as "{column_str}",
            no1 as '1着',
            no2 as "2着",
            no3 as '3着',
            no4 as '4着以下',
            concat( FORMAT(no1 / (no1 + no2 + no3 + no4) * 100, 0), "％") as "勝率",
            concat( FORMAT((no1 + no2 ) / (no1 + no2 + no3 + no4)  * 100, 0), "％") as "連対率",
            concat( FORMAT( (no1 + no2 + no3) / (no1 + no2 + no3 + no4)  * 100, 0), "％")as "3着以内"
        FROM(
            SELECT
                {column},
                MAX(CASE arrival_order WHEN 1 THEN re_count ELSE 0 END) as "no1",
                MAX(CASE arrival_order WHEN 2 THEN re_count ELSE 0 END) as "no2",
                MAX(CASE arrival_order WHEN 3 THEN re_count ELSE 0 END) as "no3",
                MAX(CASE WHEN arrival_order > 3 THEN re_count ELSE 0 END) as "no4"
            FROM
                count_tmp
            GROUP BY
                {column}
        ) AS arrival_order
      '''
    return stmt

  def frame_no(self): # 枠順別成績
    with self.pool.cursor() as cursor:
      stmt = self.__base_stmt('frame_no', '馬番')
      cursor.execute(stmt)
      return cursor.fetchall()

  def horse_no(self): # 馬番別成績
    with self.pool.cursor() as cursor:
      stmt = self.__base_stmt('horse_no', '馬番')
      cursor.execute(stmt)
      data = cursor.fetchall()
      return {
        'course_analysis_id': self.__get_course_analysis_id('umaban'),
        'data': data,
        'memo': '馬番別成績'
      }
  
  def __get_course_analysis_id(self, key :str): # course_analysis_idを生成する
    course_analysis_id = f'''{key}-{self.place_id}-{self.race_type_en}-{self.length}-{self.turf_cond_en}-{self.days}'''
    return course_analysis_id

  def insertCourseAnalysis(self, course_analysis_id, data, memo):
    corse_analysis_columns = [
      'course_analysis_id', 'place_id', 'length', 'memo', 'turf_cond', 'race_type', 'data'
    ]

    ## カラムの個数だけパーサー(%s)を用意する
    def create_parser(count):
      parser = ''
      for i in range(count):
        parser += '%s' if (i + 1) == count else '%s, '
      return parser

    data_json = json.dumps(data)
    values = (
      course_analysis_id, self.place_id, self.length, memo, self.turf_cond, self.race_type, data_json
    )

    try:
      with self.pool.cursor() as cursor:
        columns = ",".join(corse_analysis_columns)
        parser = create_parser(len(corse_analysis_columns))
        # stmt = (f'''INSERT INTO course_analysis ({columns}) VALUES ({parser})''', values)
        stmt = (f'''INSERT INTO course_analysis ({columns}) VALUES ({values})''')
        print(stmt)
        cursor.execute(stmt)
        cursor.commit()
        return True
    except Exception as e:
      print(e)
      # cursor.rollback()
      return False