import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import pymysql
import json
from typing import Tuple, List, Set, Union
from IPython.display import display
from query import Query

class Analysis():
  '''分析で使うベースクラス'''
  def __init__(self, place_id: int, is_turf: bool, length: int, grade_race: Union[str,None]) -> None:
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
    self.grade_race: Union[str,None] = grade_race
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

    self.drop_col = ['win_rate_ranking', 'rentai_rate_ranking', 'fukusho_rate_ranking', 'win_recovery_100_over', 'd_win_recovery_100_over']
  
  def setTerms(self, turf_cond: str, days: Tuple[int]) -> None: # 検索条件をセットする
    self.days: Tuple = days
    self.days_str: str = '-'.join([str(i) for i in days])
    self.turf_cond: str = turf_cond
    if turf_cond == '良':
      self.turf_cond_en: str = 'good'
    elif turf_cond == '重':
      self.turf_cond_en: str = 'bad'

  def __create_where(self) -> str: # WHERE句の条件の文字列を返す
    if self.grade_race is not None:
      stmt =  f'''
      ra.name LIKE '%{self.grade_race}%'
      AND ra.date_and_time > '2012-01-01 09:50:00'
      '''
    else:
      stmt = f'''
        ra.place_id = {self.place_id}
        AND ra.length = {self.length}
        AND ra.race_type = '{self.race_type}'
        AND ra.date_and_time > '2010-01-01 09:50:00'
        AND ra.turf_cond = '{self.turf_cond}'
        AND ra.days IN {self.days}
        -- AND ra.race_rank = 'オープン'
        '''
    return  stmt

  def count_race(self) -> str: # 対象レース数をカウント
    stmt = f'''
      SELECT count(*) as target_race
      FROM race ra
      WHERE {self.__create_where()}
    '''
    with self.pool.cursor() as cursor:
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
    return data[0]['target_race']
  
  def count_horse(self) -> str: # 対象レース数をカウント
    stmt = f'''
      SELECT count(*) as target_race
      FROM race ra
      INNER JOIN result re ON ra.race_id = re.race_id
      WHERE {self.__create_where()}
    '''
    with self.pool.cursor() as cursor:
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
    return data[0]['target_race']

  def grade_race_course(self):
    if self.grade_race is None:
      course =  ''
    else:
      stmt = f'''
        SELECT 
          mp.name,
          ra.length,
          ra.race_type,
          ra.circling,
          ra.terms
          FROM race ra
          INNER JOIN master_place mp ON ra.place_id = mp.id
          WHERE
          ra.name LIKE '%{self.grade_race}%'
          ORDER BY ra.date_and_time DESC
          LIMIT 1
      '''
      with self.pool.cursor() as cursor:
        cursor.execute(stmt)
        data: List[dict] = cursor.fetchall()
        data = data[0]

      course = f'''開催場所: {data['name']}競馬場\n距離: {data['length']}\nコース: {data['race_type']}\n周り: {data['circling']}\n条件: {data['terms']}\n'''
    return course
  
  def frame_no(self) -> dict: # 枠順別成績
    column = 'frame_no'
    column_ja = '枠番'
    case_re_column = f'''re.{column}'''
    case_column = case_re_column
    case_column_ja = f'''rate_tmp.{column}'''

    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('wakuban'),
        'data': {
          'table_header': self.__create_column_ording('枠番'),
          'data': data,
        },
        'memo': '枠番別成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  def horse_no(self) -> dict: # 馬番別成績
    column = 'horse_no'
    column_ja = '馬番'
    case_re_column = f'''re.{column}'''
    case_column = case_re_column
    case_column_ja = f'''rate_tmp.{column}'''

    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('umaban'),
        'data': {
          'table_header': self.__create_column_ording('馬番'),
          'data': data,
        },
        'memo': '馬番別成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  def popularity_order(self) -> dict: # 人気順別成績
    column = 'popularity_order'
    column_ja = '人気順'
    case_re_column = f'''re.{column}'''
    case_column = case_re_column
    case_column_ja = f'''rate_tmp.{column}'''
    
    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      print(stmt)
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('ninnki'),
        'data': {
          'table_header': self.__create_column_ording('人気'),
          'data': data,
        },
        'memo': '人気順成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  def leg_type(self) -> dict:
    column = '4c'
    column_ja = '脚質'
    case_re_column = f'''
      CASE
          WHEN re.4c = 1 THEN 1 
          WHEN re.4c between 2 AND 4 THEN 2
          WHEN re.4c between 5 AND 8 THEN 3
          WHEN re.4c >= 9 THEN 4
      END
    '''
    case_column = f'''
      CASE
          WHEN 4c = 1 THEN 1 
          WHEN 4c between 2 AND 4 THEN 2
          WHEN 4c between 5 AND 8 THEN 3
          WHEN 4c >= 9 THEN 4
      END
    '''
    case_column_ja = f'''
      CASE rate_tmp.4c
              WHEN 1 THEN '逃げ'
              WHEN 2 THEN '先行'
              WHEN 3 THEN '差し'
              WHEN 4 THEN '追込'
      END
    '''
    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('kyakusitu'),
        'data': {
          'table_header': self.__create_column_ording('脚質'),
          'data': data,
        },
        'memo': '脚質別成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  
  def horse_weight(self) -> dict: # 馬体重別成績
    column = 'horse_weight'
    column_ja = '馬体重'
    case_re_column = f'''
      CASE
        WHEN re.horse_weight < 420 THEN  1
        WHEN re.horse_weight between 420 AND 439 THEN 2
        WHEN re.horse_weight between 440 AND 459 THEN 3
        WHEN re.horse_weight between 460 AND 479 THEN 4
        WHEN re.horse_weight between 480 AND 499 THEN 5
        WHEN re.horse_weight between 500 AND 519 THEN 6
        WHEN re.horse_weight >= 520 THEN 7
      END
    '''

    case_column = f'''
      CASE
        WHEN horse_weight < 420 THEN  1
        WHEN horse_weight between 420 AND 439 THEN 2
        WHEN horse_weight between 440 AND 459 THEN 3
        WHEN horse_weight between 460 AND 479 THEN 4
        WHEN horse_weight between 480 AND 499 THEN 5
        WHEN horse_weight between 500 AND 519 THEN 6
        WHEN horse_weight >= 520 THEN 7
      END
    '''

    case_column_ja = f'''
      CASE rate_tmp.horse_weight
        WHEN 1 THEN '~ 419kg'
        WHEN 2 THEN '420kg ~ 439kg'
        WHEN 3 THEN '440kg ~459kg'
        WHEN 4 THEN '460kg ~ 479kg'
        WHEN 5 THEN '480kg ~ 499kg'
        WHEN 6 THEN '500kg ~ 519kg'
        WHEN 7 THEN '520kg ~ '
      END
    '''
    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('bataijuu'),
        'data': {
          'table_header': self.__create_column_ording('馬体重'),
          'data': data,
        },
        'memo': '馬体重別成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  def seconds_3f(self) -> dict: # 上がり3F
    column = 'seconds_3f'
    column_ja = '3Fタイム'
    case_re_column = f'''
      CASE
        WHEN re.seconds_3f between 31.5 AND 31.9 THEN  1
        WHEN re.seconds_3f between 32.0 AND 32.4 THEN  2
        WHEN re.seconds_3f between 32.5 AND 32.9 THEN  3
        WHEN re.seconds_3f between 33.0 AND 33.4 THEN 4
        WHEN re.seconds_3f between 33.5 AND 33.9 THEN 5
        WHEN re.seconds_3f between 34.0 AND 34.4 THEN 6
        WHEN re.seconds_3f between 34.5 AND 34.9 THEN 7
        WHEN re.seconds_3f between 35.0 AND 35.4 THEN 8
        WHEN re.seconds_3f between 35.5 AND 35.9 THEN 9
        WHEN re.seconds_3f >= 36.0 THEN 10
      END
    '''

    case_column = f'''
      CASE
        WHEN seconds_3f between 31.5 AND 31.9 THEN  1
        WHEN seconds_3f between 32.0 AND 32.4 THEN  2
        WHEN seconds_3f between 32.5 AND 32.9 THEN  3
        WHEN seconds_3f between 33.0 AND 33.4 THEN 4
        WHEN seconds_3f between 33.5 AND 33.9 THEN 5
        WHEN seconds_3f between 34.0 AND 34.4 THEN 6
        WHEN seconds_3f between 34.5 AND 34.9 THEN 7
        WHEN seconds_3f between 35.0 AND 35.4 THEN 8
        WHEN seconds_3f between 35.5 AND 35.9 THEN 9
        WHEN seconds_3f >= 36.0 THEN 10
      END
    '''

    case_column_ja = f'''
      CASE rate_tmp.seconds_3f
        WHEN 1 THEN '31.5 ~ 31.9'
        WHEN 2 THEN '32.0 ~ 32.4'
        WHEN 3 THEN '32.5 ~ 32.9'
        WHEN 4 THEN '33.0 ~ 33.4'
        WHEN 5 THEN '33.5 ~ 33.9'
        WHEN 6 THEN '34.0 ~  34.4'
        WHEN 7 THEN '34.5 ~ 34.9'
        WHEN 8 THEN '35.0 ~ 35.4'
        WHEN 9 THEN '35.5 ~ 35.9'
        WHEN 10 THEN '36.0 ~'
      END
    '''
    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('last3f'),
        'data': {
          'table_header': self.__create_column_ording('3Fタイム'),
          'data': data,
        },
        'memo': '上がり3Fタイム別成績',
        'df': df.drop(self.drop_col, axis=1)
      }

  def seconds_3f_rank(self) -> dict: # 人気順別成績
    column = 'seconds_3f'
    column_ja = '3F速さ順位'
    case_re_column = f'''re.{column}'''
    case_column = case_re_column
    case_column_ja = f'''rate_tmp.{column}'''
    
    with self.pool.cursor() as cursor:
      stmt: str = Query.base_stmt_rank(
        column, case_column, case_re_column, column_ja, case_column_ja, self.__create_where()
      )
      print(stmt)
      cursor.execute(stmt)
      data: List[dict] = cursor.fetchall()
      data = self.processingData(data)
      df = pd.read_sql(stmt, self.pandas_pool)
      return {
        'course_analysis_id': self.__get_analysis_key('3f_rank'),
        'data': {
          'table_header': self.__create_column_ording('3F速さ順位'),
          'data': data,
        },
        'memo': '3F速さ順位',
        'df': df.drop(self.drop_col, axis=1)
      }
  def processingData(self, data) -> List[List]: # クエリの結果を加工
    data = [self.__proccessing_dict_value(d) for d in data]
    data = [self.__rank_coloring(d) for d in data]
    return data

  def insertCourseAnalysis(self, analysis_key: str, data: dict, memo: str) -> bool: # 分析結果をDBにインサート
    corse_analysis_columns: List = [
      'analysis_key', 'place_id', 'length', 'memo', 'turf_cond', 'race_type', 'days', 'data'
    ]

    data_json = json.dumps(data)
    values: Tuple = (
      analysis_key, self.place_id, self.length, memo, self.turf_cond, self.race_type, self.days_str ,data_json
    )

    try:
      with self.pool.cursor() as cursor:
        print(analysis_key)
        # print(memo)
        # print(data)
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

  def __proccessing_dict_value(self, data) -> dict: # valueをフォーマット
    for k, v in data.items():
      data[k] = {
        'value': v,
        'schema': None
      }
    return data
  
  def __rank_coloring(self, data) -> dict: # ランキング順に色付けする
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
    data = target_column_rank(data, 'win_recovery_100_over', '単回値')
    data = target_column_rank(data, 'd_win_recovery_100_over', '複回値')
    return data
  
  def __create_column_ording(self, column_ja: str) -> str: # 表のカラムの表示順を管理
    return [column_ja,'着度数','勝率','連対率','複勝率','単回値','複回値']