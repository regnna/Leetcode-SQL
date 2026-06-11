import pandas as pd
import duckdb

hall_events_data = [
    [1, '2023-01-13', '2023-01-14'],
    [1, '2023-01-14', '2023-01-17'],
    [1, '2023-01-18', '2023-01-25'],
    [2, '2022-12-09', '2022-12-23'],
    [2, '2022-12-13', '2022-12-17'],
    [3, '2022-12-01', '2023-01-30']
]

hallevents = pd.DataFrame(
    hall_events_data,
    columns=['hall_id', 'start_day', 'end_day']
).astype({
    'hall_id': 'int64',
    'start_day': 'datetime64[ns]',
    'end_day':'datetime64[ns]'
})

print(duckdb.query("""

with cte as(
select *,max(end_day) 
over(partition by hall_id order by start_day Rows between unbounded preceding and 1 preceding)
as max_end_day
from hallevents
),
cte2 as(
select *, case when start_day>max_end_day then 1 else 0 end as val
from cte
),
cte3 as(
select *,sum(val) over(partition by hall_id order by start_day
rows between unbounded preceding and current row) as grp
from cte2)

select hall_id,min(start_day) as start_day, max(end_day) as end_day

from cte3 group by hall_id,grp
""").to_df())

