import pandas as pd
import duckdb

tasks_data = [
    [1, 1, '2022-06-13'],  # Monday
    [2, 6, '2022-06-14'],  # Tuesday
    [3, 6, '2022-06-15'],  # Wednesday
    [4, 3, '2022-06-18'],  # Saturday
    [5, 5, '2022-06-19'],  # Sunday
    [6, 7, '2022-06-19']   # Sunday
]

Tasks = pd.DataFrame(
    tasks_data,
    columns=['task_id', 'assignee_id', 'submit_date']
).astype({
    'task_id': 'int64',
    'assignee_id': 'int64',
    'submit_date': 'datetime64[ns]'
})

print(duckdb.query("""
with cte as(
select *,if((dayofweek(submit_date)==0 or dayofweek(submit_date)==6),2,1) as day from Tasks
)

select cast(sum(if(day=2,day,0))/2 as int) as weekend_cnt,
cast(sum(if(day=1,day,0)) as int) as working_cnt from cte
""").to_df())

