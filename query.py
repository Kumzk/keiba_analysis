import os

class Query:
  """
  クエリをまとめたクラスです
  """

  @classmethod
  def base_stmt(cls, column: str, case_column: str,case_re_column: str ,column_ja: str, case_column_ja: str,where: str) -> str: #ベースのSQL
    stmt: str = f'''
        WITH count_tmp AS(
          SELECT
            {case_re_column} AS {column},
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
              SUM(CASE arrival_order WHEN 1 THEN re_count ELSE 0 END) as "no1",
              SUM(CASE arrival_order WHEN 2 THEN re_count ELSE 0 END) as "no2",
              SUM(CASE arrival_order WHEN 3 THEN re_count ELSE 0 END) as "no3",
              SUM(CASE WHEN arrival_order > 3 THEN re_count ELSE 0 END) as "no4"
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
          winning_percentage_tmp AS (
            SELECT
            {case_column} AS {column},
            refund.refund as refund
            FROM
              race ra
            INNER JOIN result re
              ON ra.race_id = re.race_id
            LEFT OUTER JOIN refund
              ON ra.race_id = refund.race_id
              AND re.horse_no = refund.horse_no
              AND refund.ticket_type = 1
            WHERE {where}
          ),
            winning_percentage AS (
            SELECT
                {column},
                concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'win_recovery_rate',
                SUM(refund) / (count(*) * 100) * 100 as 'win_recovery_rate_num'
            FROM
                winning_percentage_tmp
            GROUP BY
                {column}
            ORDER BY {column}
        ),
        d_winning_percentage_tmp AS (
          SELECT
          {case_column} AS {column},
          refund.refund as refund
          FROM
            race ra
          INNER JOIN result re
            ON ra.race_id = re.race_id
          LEFT OUTER JOIN refund
            ON ra.race_id = refund.race_id
            AND re.horse_no = refund.horse_no
            AND refund.ticket_type = 2
          WHERE {where}
      ),
        d_win_recovery_rate AS (
          SELECT
              {column} AS {column}_1,
              concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'd_win_recovery_rate',
              SUM(refund) / (count(*) * 100) * 100 as 'd_win_recovery_rate_num'
          FROM
              d_winning_percentage_tmp
          GROUP BY
              {column}
          ORDER BY {column}
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
              {case_column_ja} AS {column_ja},
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
  
  @classmethod
  def base_stmt_rank(cls, column: str, case_column: str,case_re_column: str ,column_ja: str, case_column_ja: str,where: str) -> str: #ベースのSQL
    stmt: str = f'''
        WITH rank_tmp AS (
          SELECT 
              re.{column} AS {column},
                  RANK() OVER ( partition by re.race_id order by re.{column}) as {column}_rank,
              re.race_id as race_id,
              re.arrival_order as arrival_order,
              re.seq as seq
          FROM race ra
          INNER JOIN result re ON ra.race_id = re.race_id
          WHERE {where}
        ),
        count_tmp AS(
          SELECT
            {column}_rank AS {column},
            arrival_order as arrival_order,
            count(seq) as re_count
          FROM rank_tmp
          GROUP BY
            {column}_rank,
            arrival_order
          ORDER BY {column}_rank
        ),
        arrival_tmp AS (
          SELECT
              {column},
              SUM(CASE arrival_order WHEN 1 THEN re_count ELSE 0 END) as "no1",
              SUM(CASE arrival_order WHEN 2 THEN re_count ELSE 0 END) as "no2",
              SUM(CASE arrival_order WHEN 3 THEN re_count ELSE 0 END) as "no3",
              SUM(CASE WHEN arrival_order > 3 THEN re_count ELSE 0 END) as "no4"
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
          winning_percentage_tmp AS (
            SELECT
            RANK() OVER ( partition by re.race_id order by re.{column}) as {column},
            refund.refund as refund
            FROM
              race ra
            INNER JOIN result re
              ON ra.race_id = re.race_id
            LEFT OUTER JOIN refund
              ON ra.race_id = refund.race_id
              AND re.horse_no = refund.horse_no
              AND refund.ticket_type = 1
            WHERE {where}
          ),
            winning_percentage AS (
            SELECT
                {column},
                concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'win_recovery_rate',
                SUM(refund) / (count(*) * 100) * 100 as 'win_recovery_rate_num'
            FROM
                winning_percentage_tmp
            GROUP BY
                {column}
            ORDER BY {column}
        ),
        d_winning_percentage_tmp AS (
          SELECT
          RANK() OVER ( partition by re.race_id order by re.{column}) as {column},
          refund.refund as refund
          FROM
            race ra
          INNER JOIN result re
            ON ra.race_id = re.race_id
          LEFT OUTER JOIN refund
            ON ra.race_id = refund.race_id
            AND re.horse_no = refund.horse_no
            AND refund.ticket_type = 2
          WHERE {where}
      ),
        d_win_recovery_rate AS (
          SELECT
              {column} AS {column}_1,
              concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'd_win_recovery_rate',
              SUM(refund) / (count(*) * 100) * 100 as 'd_win_recovery_rate_num'
          FROM
              d_winning_percentage_tmp
          GROUP BY
              {column}
          ORDER BY {column}
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
              {case_column_ja} AS {column_ja},
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
  

  @classmethod
  def base_join_horse_jokey_stmt(cls, column: str, case_column: str,case_re_column: str ,column_ja: str, case_column_ja: str,where: str) -> str: #ベースのSQL
    stmt: str = f'''
        WITH count_tmp AS(
          SELECT
            {case_re_column} AS {column},
            re.arrival_order as arrival_order,
            count(re.seq) as re_count
          FROM race ra
          INNER JOIN result re ON ra.race_id = re.race_id
          INNER JOIN horse h ON re.horse_id =  h.id
          INNER JOIN jockey j ON re.jockey_id =  j.id
          WHERE {where}
          GROUP BY
            {case_re_column} ,
            re.arrival_order
          ORDER BY {case_re_column} 
        ),
        arrival_tmp AS (
          SELECT
              {column},
              SUM(CASE arrival_order WHEN 1 THEN re_count ELSE 0 END) as "no1",
              SUM(CASE arrival_order WHEN 2 THEN re_count ELSE 0 END) as "no2",
              SUM(CASE arrival_order WHEN 3 THEN re_count ELSE 0 END) as "no3",
              SUM(CASE WHEN arrival_order > 3 THEN re_count ELSE 0 END) as "no4"
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
          winning_percentage_tmp AS (
            SELECT
            {case_column} AS {column},
            refund.refund as refund
            FROM
              race ra
            INNER JOIN result re ON ra.race_id = re.race_id
            INNER JOIN horse h ON re.horse_id =  h.id
            INNER JOIN jockey j ON re.jockey_id =  j.id
            LEFT OUTER JOIN refund
              ON ra.race_id = refund.race_id
              AND re.horse_no = refund.horse_no
              AND refund.ticket_type = 1
            WHERE {where}
          ),
            winning_percentage AS (
            SELECT
                {column} AS {column}_1,
                concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'win_recovery_rate',
                SUM(refund) / (count(*) * 100) * 100 as 'win_recovery_rate_num'
            FROM
                winning_percentage_tmp
            GROUP BY
                {column}
            ORDER BY {column}
        ),
        d_winning_percentage_tmp AS (
          SELECT
          {case_column} AS {column},
          refund.refund as refund
          FROM
            race ra
          INNER JOIN result re ON ra.race_id = re.race_id
          INNER JOIN horse h ON re.horse_id =  h.id
          INNER JOIN jockey j ON re.jockey_id =  j.id
          LEFT OUTER JOIN refund
            ON ra.race_id = refund.race_id
            AND re.horse_no = refund.horse_no
            AND refund.ticket_type = 2
          WHERE {where}
      ),
        d_win_recovery_rate AS (
          SELECT
              {column} AS {column}_2,
              concat( FORMAT(SUM(refund) / (count(*) * 100) * 100, 1), '%') as 'd_win_recovery_rate',
              SUM(refund) / (count(*) * 100) * 100 as 'd_win_recovery_rate_num'
          FROM
              d_winning_percentage_tmp
          GROUP BY
              {column}
          ORDER BY {column}
        ), recover_tmp AS (
          SELECT
            winning_percentage.{column}_1 as name,
            win_recovery_rate,
            d_win_recovery_rate,
            CASE WHEN winning_percentage.win_recovery_rate_num >= 100 THEN  1 ELSE 0 END AS 'win_recovery_100_over',
            CASE WHEN d_win_recovery_rate.d_win_recovery_rate_num >= 100 THEN  1 ELSE 0 END AS 'd_win_recovery_100_over'
          FROM 
              winning_percentage
          INNER JOIN d_win_recovery_rate ON 
              winning_percentage.{column}_1 = d_win_recovery_rate.{column}_2
        ),
        result_tmp AS (
          SELECT
              {case_column_ja} AS {column_ja},
              concat(no1, '-', no2, '-', no3, '-', no4, '/', (no1 + no2 + no3 + no4)) as '着度数',
              concat( FORMAT(win_rate, 1), '%') as '勝率',
              concat( FORMAT(rentai_rate, 1), '%') as '連対率',
              concat( FORMAT(fukusho_rate, 1), '%') as '複勝率',
              RANK() OVER(ORDER BY win_rate DESC) AS win_rate_ranking,
              RANK() OVER(ORDER BY rentai_rate DESC) AS rentai_rate_ranking,
              RANK() OVER(ORDER BY fukusho_rate DESC) AS fukusho_rate_ranking,
              win_recovery_rate as '単回値',
              d_win_recovery_rate as '複回値',
              win_recovery_100_over,
              d_win_recovery_100_over,
              (no1 + no2 + no3 + no4) as total_count
          FROM
            rate_tmp
          INNER JOIN recover_tmp ON rate_tmp.{column} = recover_tmp.{column}
          ORDER BY rate_tmp.{column}
        )
        SELECT
            *
        FROM
          result_tmp
        WHERE
          total_count >= 20
        ORDER BY win_rate_ranking
      '''
    return stmt
  