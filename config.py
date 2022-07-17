from typing import Tuple, List, Set

grade_race = 'トヨタ賞中京記念'
# grade_race = None # 重賞分析の時はコメントアウトする

"""
開催場所
1	札幌, 2	函館, 3	福島, 4	新潟, 5	東京, 6	中山, 7	中京, 8	京都, 9	阪神, 10 小倉
"""
place = {1: '札幌', 2:'函館', 3:'福島', 4:'新潟', 5:'東京', 6:'中山', 7:'中京', 8:'京都', 9:'阪神', 10:'小倉'}
place_id: int = 2 # 上記から競馬場を選択

### 芝 or ダート　使う方をコメントイン
is_turf: bool = True  # 芝
# is_turf: bool = False # ダート
curse = '芝' if is_turf else 'ダート'

### 距離
length: int = 1200

### 馬場コンディション 使う方をコメントイン
# cond: str = '良'
cond: str = '重'

### 開催日のいつか
days_pattern: dict = {
  1: (1, 2), # 開幕週 
  2: (3, 4), # 2週目だけ
  3: (5, 6), # 3週目だけ
  4: (7, 8), # 最終週
  5: (1,2,3,4), #開催前半
  6: (5,6,7,8)  #開催後半
}
display_day = {1:'開幕週', 2:'2週目だけ', 3:'3週目だけ', 4: '最終週', 5:'開催前半', 6:'開催後半'}
select_day = 4
days: Tuple = days_pattern[select_day]

if grade_race is not None:
  print(f'重賞分析:{grade_race}')
else:
  print(f'開催場所: {place[place_id]}競馬場')
  print(f'コース: {curse}')
  print(f'距離: {length}m')
  print(f'馬場状態:{cond}')
  print(f'開催期間: {display_day[select_day]}')