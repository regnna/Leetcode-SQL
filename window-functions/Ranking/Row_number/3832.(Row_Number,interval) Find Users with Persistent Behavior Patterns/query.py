import pandas as pd
import duckdb

activity_data = [
    [1, '2024-01-01', 'login'],
    [1, '2024-01-02', 'login'],
    [1, '2024-01-03', 'login'],
    [1, '2024-01-04', 'login'],
    [1, '2024-01-05', 'login'],
    [1, '2024-01-06', 'logout'],

    [2, '2024-01-01', 'click'],
    [2, '2024-01-02', 'click'],
    [2, '2024-01-03', 'click'],
    [2, '2024-01-04', 'click'],

    [3, '2024-01-01', 'view'],
    [3, '2024-01-02', 'view'],
    [3, '2024-01-03', 'view'],
    [3, '2024-01-04', 'view'],
    [3, '2024-01-05', 'view'],
    [3, '2024-01-06', 'view'],
    [3, '2024-01-07', 'view'],
]

activity = pd.DataFrame(
    activity_data,
    columns=['user_id', 'action_date', 'action']
).astype({
    'user_id': 'int64',
    'action_date': 'datetime64[ns]',
    'action': 'string'
})


print(duckdb.query(""" 
with cte as(
select *,row_number() over(partition by user_id,action order by action_date) as rnk from activity
)
,cte2 as(
select *,(action_date - (rnk * interval '1' DAY)) as diff from cte

),cte3 as(

select user_id,action,count(*) as streak,min(action_date) as start_date ,max(action_date) end_date ,
/* now we nned to compare witih a single users multiple streak we need to choose biggest streak out of them as well */
row_number() over(partition by user_id,action order by count(*) desc) as rnk2
from cte2
group by user_id,action,diff having count(*)>=5
)

select user_id,action,streak as streak_length,start_date,end_date from cte3 where rnk2 order by streak_length desc,user_id
""").to_df())