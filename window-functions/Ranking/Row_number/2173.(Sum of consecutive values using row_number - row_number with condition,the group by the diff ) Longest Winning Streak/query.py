import pandas as pd
import duckdb

matches_data = [
    [1, '2022-01-17', 'Win'],
    [1, '2022-01-18', 'Win'],
    [1, '2022-01-25', 'Win'],
    [1, '2022-01-31', 'Draw'],
    [1, '2022-02-08', 'Win'],
    [2, '2022-02-06', 'Lose'],
    [2, '2022-02-08', 'Lose'],
    [3, '2022-03-30', 'Win']
]

matches = pd.DataFrame(
    matches_data,
    columns=['player_id', 'match_day', 'result']

).astype({
    'player_id':'int64',
    'match_day':'datetime64[ns]',
    'result':'string'
})


print(duckdb.query("""
/*select *,if(result='Win',SUM(CASE WHEN result='Win' THEN 1 ELSE 0 END) 
            OVER (partition by player_id ORDER BY match_day ROWS UNBOUNDED PRECEDING),0) AS group_id
            
             from matches*/

with cte as(
select *,row_number() over(partition by player_id order by match_day) rn,row_number() over(partition by player_id,result order by match_day) rn_win
 from matches
),
cte2 as(
select player_id,rn-rn_win as diff,count(*) as streak,row_number() over(partition by player_id order by count(*) desc) as rn
from cte where result ='Win'
group by player_id,(rn-rn_win)
),
c3 as (
select * from cte2 where rn=1
)

select distinct player_id,ifnull(streak,0) as longest_streak from matches m left join c3 using(player_id)


""").to_df())