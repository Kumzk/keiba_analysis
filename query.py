import os

class Query:
  """
  クエリをまとめたクラスです
  """

  @classmethod
  def base_stmt(cls, column: str, column_ja: str, where: str) -> str: #ベースのSQL
    stmt: str = f'''
        WITH count_tmp AS(
          SELECT
            re.{column} as {column},
            re.arrival_order as arrival_order,
            count(re.seq) as re_count
          FROM race ra
          INNER JOIN result re ON ra.race_id = re.race_id
          WHERE {where}
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
          ),
            winning_percentage AS (
            SELECT
                re.{column} as {column},
                concat( FORMAT(SUM(refund.refund) / (count(*) * 100) * 100, 1), '%') as 'win_recovery_rate',
                SUM(refund.refund) / (count(*) * 100) * 100 as 'win_recovery_rate_num'
            FROM
                race ra
                INNER JOIN result re
                    ON ra.race_id = re.race_id
                LEFT OUTER JOIN refund
                    ON ra.race_id = refund.race_id
                    AND re.horse_no = refund.horse_no
                    AND refund.ticket_type = 1
            WHERE {where}
            GROUP BY
                re.{column}
            ORDER BY re.{column}
        ), d_win_recovery_rate AS (
          SELECT
              re.{column} as {column}_1,
              concat( FORMAT(SUM(refund.refund) / (count(*) * 100) * 100, 1), '%') as 'd_win_recovery_rate',
              SUM(refund.refund) / (count(*) * 100) * 100 as 'd_win_recovery_rate_num'
          FROM
              race ra
              INNER JOIN result re
                  ON ra.race_id = re.race_id
              LEFT OUTER JOIN refund
                  ON ra.race_id = refund.race_id
                  AND re.horse_no = refund.horse_no
                  AND refund.ticket_type = 2
          WHERE
              {where}
          GROUP BY
              re.{column}
          ORDER BY re.{column}
        ), recover_tmp AS (
          SELECT
            winning_percentage.{column},
            win_recovery_rate,
            d_win_recovery_rate,
            CASE WHEN winning_percentage.win_recovery_rate_num >= 100 THEN  1 ELSE 0 END AS 'win_recovery_100_over',
            CASE WHEN d_win_recovery_rate.d_win_recovery_rate_num >= 100 THEN  1 ELSE 0 END AS 'd_win_recovery_100_over'
          FROM 
              winning_percentage
          INNER JOIN d_win_recovery_rate ON 
              winning_percentage.{column} = d_win_recovery_rate.{column}_1
        )
          SELECT 
              rate_tmp.{column} as {column_ja},
              concat(no1, '-', no2, '-', no3, '-', no4, '/', (no1 + no2 + no3 + no4)) as '着度数',
              -- no1 as '1着',no2 as '2着',no3 as '3着',no4 as '4着以下',
              concat( FORMAT(win_rate, 1), '%') as '勝率',
              concat( FORMAT(rentai_rate, 1), '%') as '連対率',
              concat( FORMAT(fukusho_rate, 1), '%') as '複勝率',
              RANK() OVER(ORDER BY win_rate DESC) AS win_rate_ranking,
              RANK() OVER(ORDER BY rentai_rate DESC) AS rentai_rate_ranking,
              RANK() OVER(ORDER BY fukusho_rate DESC) AS fukusho_rate_ranking,
              win_recovery_rate as '単回値',
              d_win_recovery_rate as '複回値',
              win_recovery_100_over,
              d_win_recovery_100_over
          FROM
            rate_tmp
          INNER JOIN recover_tmp ON rate_tmp.{column} = recover_tmp.{column}
          ORDER BY rate_tmp.{column}
      '''
    return stmt