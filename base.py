import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import pymysql
import json
from typing import Tuple, List, Set

class Analysis():
  '''分析で使うベースクラス'''
  def __init__(self, place_id: int, is_turf: bool, length: int) -> None:
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
    place: dict = {1:	'sapporo', 2: 'hakodate', 3: 'fukushima', 4: 'niigata', 5: 'tokyo', 6: 'nakayama', 7: 'chukyo', 8: 'kyoto', 9: 'hanshin', 10: 'kokura'}
    self.place: str = place[place_id]
    self.place_id: int = place_id
    self.race_type: str = '芝' if is_turf else 'ダ'
    self.race_type_en: str = 'turf' if is_turf else 'dirt'
    self.length: int = length

    load_dotenv()
    user: str = os.environ.get('user')
    password: str = os.environ.get('password')
    host: str = os.environ.get('host')
    port: int = os.environ.get('port')
    database: str = os.environ.get('database')

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
    url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    self.pandas_pool = sqlalchemy.create_engine(url)
  
  def setTerms(self, turf_cond: str, days: Tuple[int]) -> None:
    self.days: Tuple = days
    self.days_str: str = '-'.join([str(i) for i in days])
    self.turf_cond: str = turf_cond
    if turf_cond == '良':
      self.turf_cond_en: str = 'good'
    elif turf_cond == '重':
      self.turf_cond_en: str = 'bad'

  def __base_stmt(self, column: str, column_ja: str) -> str: #ベースのSQL
    stmt: str = f'''
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
              AND ra.days IN {self.days}
              -- AND ra.race_rank = 'オープン'
          GROUP BY
            re.{column},
            re.arrival_order
          ORDER BY re.{column}
        ),
        arrival_tmp AS (
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
          ),
          rate_tmp AS (
            SELECT
                *,
                no1 / (no1 + no2 + no3 + no4) * 100 as win_rate,
                (no1 + no2 ) / (no1 + no2 + no3 + no4)  * 100 as rentai_rate,
                (no1 + no2 + no3) / (no1 + no2 + no3 + no4)  * 100 as fukusho_rate
            FROM
              arrival_tmp
            ORDER BY
              {column}
          )
          SELECT 
              {column} as {column_ja},
              no1 as '1着',
              no2 as '2着',
              no3 as '3着',
              no4 as '4着以下',
              concat( FORMAT(win_rate, 1), '%') as '勝率',
              concat( FORMAT(rentai_rate, 1), '%') as '連対率',
              concat( FORMAT(fukusho_rate, 1), '%') as '複勝率',
              RANK() OVER(ORDER BY win_rate DESC) AS win_rate_ranking,
              RANK() OVER(ORDER BY rentai_rate DESC) AS rentai_rate_ranking,
              RANK() OVER(ORDER BY fukusho_rate DESC) AS fukusho_rate_ranking
          FROM
            rate_tmp
      '''
    return stmt

  def frame_no(self) -> dict: # 枠順別成績
    with self.pool.cursor() as cursor:
      stmt: str = self.__base_stmt('frame_no', '馬番')
      data: List[dict] = cursor.execute(stmt)
      return {
        'course_analysis_id': self.__get_analysis_key('umaban'),
        'data': data,
        'memo': '枠番別成績'
      }

  def horse_no(self) -> dict: # 馬番別成績
    with self.pool.cursor() as cursor:
      stmt: str = self.__base_stmt('horse_no', '馬番')
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      return {
        'course_analysis_id': self.__get_analysis_key('umaban'),
        'data': data,
        'memo': '馬番別成績',
        'column_ordering': self.__create_column_ording('馬番')
      }
  
  def processingData(self, data) -> List[List]:
    data = [self.__proccessing_dict_value(d) for d in data]
    data = [self.__rank_coloring(d) for d in data]
    return data

  def insertCourseAnalysis(self, analysis_key: str, data: dict, memo: str, column_ordering: str) -> bool:
    corse_analysis_columns: List = [
      'analysis_key', 'place_id', 'length', 'memo', 'turf_cond', 'race_type', 'days', 'data', 'column_ordering'
    ]

    data_json = json.dumps(data)
    values: Tuple = (
      analysis_key, self.place_id, self.length, memo, self.turf_cond, self.race_type, self.days_str ,data_json, column_ordering
    )

    try:
      with self.pool.cursor() as cursor:
        print(analysis_key)
        print(memo)
        print(data)
        columns: str = ",".join(corse_analysis_columns)
        parser: str = self.__create_parser(len(corse_analysis_columns))
        stmt: Tuple = (f'''INSERT INTO analysis ({columns}) VALUES ({parser})''', values)
        cursor.execute(*stmt)
        self.pool.commit()
        print('インサート成功')
        return True
    except Exception as e:
      print(e)
      self.pool.rollback()
      return False

  def __get_analysis_key(self, key :str) -> str: # analysis_keyを生成する
    analysis_key: str= f'''{key}-{self.place}-{self.race_type_en}-{self.length}-{self.turf_cond_en}-{self.days_str}'''
    return analysis_key

  def __create_parser(self, count) -> str: ## カラムの個数だけパーサー(%s)を用意する
    parser: str = ''
    for i in range(count):
      parser += '%s' if (i + 1) == count else '%s, '
    return parser

  def __proccessing_dict_value(self, data) -> dict:
    for k, v in data.items():
      data[k] = {
        'value': v,
        'schema': None
      }
    return data
  
  def __rank_coloring(self, data) -> dict:
    def target_column_rank(data: dict, column: str, column_ja: str):
      if data[column]['value'] == 1:
        data[column_ja]['schema'] = 'yellow'
      elif data[column]['value'] == 2:
        data[column_ja]['schema'] = 'blue'
      elif data[column]['value'] == 3:
        data[column_ja]['schema'] = 'red'
      
      del data[column]
      return data

    data = target_column_rank(data, 'win_rate_ranking', '勝率')
    data = target_column_rank(data, 'rentai_rate_ranking', '連対率')
    data = target_column_rank(data, 'fukusho_rate_ranking', '複勝率')
    return data
  
  def __create_column_ording(self, column_ja: str) -> str:
    return f'{column_ja},1着,2着,3着,4着以下,勝率,連対率,複勝率'