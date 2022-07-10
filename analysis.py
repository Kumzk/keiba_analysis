import os
from base import Analysis
import json
from typing import Tuple, List, Set
import pandas as pd
from tabulate import tabulate
import copy
print("処理スタート")

"""
開催場所
1	札幌, 2	函館, 3	福島, 4	新潟, 5	東京, 6	中山, 7	中京, 8	京都, 9	阪神, 10 小倉
"""
place_id: int = 9 # 上記から競馬場を選択

### 距離
length: int = 1800

### 芝 or ダート　使う方をコメントイン
is_turf: bool = True  # 芝
# is_turf: bool = False # ダート

### 馬場コンディション 使う方をコメントイン
cond: str = '良'
# cond: str = '重'

### 開催日のいつか
days_pattern: dict = {
  1: (1, 2), # 開幕週 
  2: (3, 4), # 2週目だけ
  3: (5, 6), # 3週目だけ
  4: (7, 8), # 最終週
  5: (1,2,3,4), #開催前半
  6: (5,6,7,8)  #開催後半
}

days: Tuple = days_pattern[2] 

# コース設定
analysis = Analysis(place_id, is_turf, length)
# 条件追加
analysis.setTerms(cond, days)
# horse_no: dict = analysis.horse_no()
# print(horse_no)
frame_no: dict = analysis.frame_no()
insert: bool = analysis.insertCourseAnalysis(frame_no['course_analysis_id'], frame_no['data'], frame_no['memo'])

# horse_no_data: dict = analysis.processingData(horse_no['data'])
# print(json.dumps(horse_no_data,indent=2, ensure_ascii=False))
# insert: bool = analysis.insertCourseAnalysis(horse_no['course_analysis_id'], horse_no['data'], horse_no['memo'])
# insert: bool = analysis.insertCourseAnalysis(horse_no['course_analysis_id'], horse_no['data'], horse_no['memo'])
# print(insert)
# print(json.dumps(horse_no))
