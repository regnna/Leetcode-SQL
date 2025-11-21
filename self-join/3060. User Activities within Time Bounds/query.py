"""
SQL Supports Timestampdiff to calculate difference between two time stamp
SELECT TIMESTAMPDIFF(second, '2023-01-01 10:00:00', '2023-01-01 15:30:00');
but duckdb as its alternate date_diff
"""


import pandas as pd
import duckdb 


data = [[101, '2023-11-01 08:00:00', '2023-11-01 09:00:00', 1, 'Viewer'], [101, '2023-11-01 10:00:00', '2023-11-01 11:00:00', 2, 'Streamer'], [102, '2023-11-01 13:00:00', '2023-11-01 14:00:00', 3, 'Viewer'], [102, '2023-11-01 15:00:00', '2023-11-01 16:00:00', 4, 'Viewer'], [101, '2023-11-02 09:00:00', '2023-11-02 10:00:00', 5, 'Viewer'], [102, '2023-11-02 12:00:00', '2023-11-02 13:00:00', 6, 'Streamer'], [101, '2023-11-02 13:00:00', '2023-11-02 14:00:00', 7, 'Streamer'], [102, '2023-11-02 16:00:00', '2023-11-02 17:00:00', 8, 'Viewer'], [103, '2023-11-01 08:00:00', '2023-11-01 09:00:00', 9, 'Viewer'], [103, '2023-11-02 20:00:00', '2023-11-02 23:00:00', 10, 'Viewer'], [103, '2023-11-03 09:00:00', '2023-11-03 10:00:00', 11, 'Viewer']]
sessions = pd.DataFrame(data,columns=['user_id', 'session_start', 'session_end', 'session_id', 'session_type']).astype({'user_id': 'Int64', 'session_start': 'datetime64[ns]', 'session_end': 'datetime64[ns]', 'session_id': 'Int64', 'session_type': 'object'})

print(duckdb.query("""
with cte as (
select s1.user_id,s1.session_id,s1.session_type,
s2.session_id,s2.session_type, date_diff('SECOND',s1.session_end,s2.session_start)/3600 as diff
from sessions as s1
inner join sessions as s2
on s1.user_id=s2.user_id
and s1.session_type=s2.session_type
and s1.session_start<s2.session_start
where 
date_diff('SECOND',s1.session_end,s2.session_start)/3600 between 0 and 12
)
select distinct(cte.user_id) from cte order by cte.user_id
/* select * from sessions*/
"""
).to_df())