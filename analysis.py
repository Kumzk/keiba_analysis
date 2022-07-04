import os
from base import Analysis
import json

print("処理スタート")

"""
開催場所
1	札幌, 2	函館, 3	福島, 4	新潟, 5	東京, 6	中山, 7	中京, 8	京都, 9	阪神, 10 小倉
"""
place = 9 # 上記から競馬場を選択

### 距離
length = 1800

### 芝 or ダート　使う方をコメントイン
is_turf = True  # 芝
# is_turf = False # ダート

### 馬場コンディション 使う方をコメントイン
cond = '良'
# cond = '重'

### 開催日のいつか
days = 1 # 開催前半
# days = 2 # 開催後半
# days = 3 # 全期間

# コース設定
analysis = Analysis(place, is_turf, length)
# 条件追加
analysis.setTerms(cond, days)
frame_no = analysis.frame_no()
horse_no  = analysis.horse_no()
insert = analysis.insertCourseAnalysis(horse_no['course_analysis_id'], horse_no['data'], horse_no['memo'])
print(insert)
# print(json.dumps(horse_no))
