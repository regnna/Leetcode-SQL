import pandas as pd
import duckdb


# Signups table
signups_data = [
    [3, '2020-03-21 10:16:13'],
    [7, '2020-01-04 13:57:59'],
    [2, '2020-07-29 23:09:44'],
    [6, '2020-12-09 10:39:37'],
    [8,'2021-02-28 23:59:55']
]

signups = pd.DataFrame(
    signups_data,
    columns=['user_id', 'time_stamp']
).astype({
    'user_id': 'int64',
    'time_stamp': 'datetime64[ns]'
})


# Confirmations table
confirmations_data = [
    [3, '2021-01-06 03:30:46', 'timeout'],
    [3, '2021-07-14 14:00:00', 'timeout'],
    [7, '2021-06-12 11:57:29', 'confirmed'],
    [7, '2021-06-13 12:58:28', 'confirmed'],
    [7, '2021-06-14 13:59:27', 'confirmed'],
    [2, '2021-01-22 00:00:00', 'confirmed'],
    [2, '2021-02-28 23:59:59', 'timeout'],
    [8, '2021-02-28 23:59:56', 'confirmed'],
    [8, '2021-02-28 23:59:57', 'timeout'],
    [8, '2021-02-28 23:59:58', 'timeout']
]

confirmations = pd.DataFrame(
    confirmations_data,
    columns=['user_id', 'time_stamp', 'action']
).astype({
    'user_id': 'int64',
    'time_stamp': 'datetime64[ns]',
    'action': 'string'   # ENUM-like
})


print(duckdb.query("""
with cte as(
select user_id,
round(sum(if(action='confirmed',1,0))/sum(count(*)) over(partition by user_id),2) as confirmation_rate
/*ROUND(
    COALESCE(
      SUM(CASE WHEN action = 'confirmed' THEN 1 ELSE 0 END) * 1.0
      / COUNT(action),
      0
    ),
    2
  ) AS confirmation_rate*/
from confirmations group by user_id
)

select user_id,coalesce(c.confirmation_rate,0)
from cte c right join signups s using(user_id)
/*
select s.user_id,case when c.user_id is null then 0
    else sum(if(c.action='confirmed',1,0))/count(*) over(partition by c.user_id) end as confirmation_rate
    from confirmations c left join signups s using(user_id) 
*/
""").to_df())