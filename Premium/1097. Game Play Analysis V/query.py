"""
1. using Dense ranking with partition over player_id and order_by event_date
2. using lead window funciton 
        The LEAD(event_date,1,NaT) window function takes a column 
        and an integer offset as arguments 
        and returns the value of the cell in that column that is the specified number of rows after the current row. 
        A third argument can be added to fill cells that do not have a corresponding row.
3. ROUND(SUM(CASE WHEN DATEDIFF('day', event_date, nxt_login) = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
"""

import pandas as pd
import duckdb

data = [[1, 2, '2016-03-01', 5], [1, 2, '2016-03-02', 6], [2, 3, '2017-06-25', 1], [3, 1, '2016-03-01', 0], [3, 4, '2018-07-03', 5]]
activity = pd.DataFrame(data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype({'player_id':'Int64', 'device_id':'Int64', 'event_date':'datetime64[ns]', 'games_played':'Int64'})

print(duckdb.query("""
with cte as
(    select *,Dense_rank() over(partition by player_id order by event_date) as rnk,
    lead(event_date,1) over(partition by player_id order by event_date) as nxt_login
    from activity
)

--select * from cte;--
select event_date as install_dt,
count(*) as installs,
ROUND(SUM(CASE WHEN DATEDIFF('day', event_date, nxt_login) = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS Day1_retention
from cte 
where rnk=1
group by event_date;

""").to_df())
