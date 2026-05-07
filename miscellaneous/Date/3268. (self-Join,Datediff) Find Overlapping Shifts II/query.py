import pandas as pd
import duckdb

employee_shifts_data = [
    [1, "2023-10-01 09:00:00", "2023-10-01 17:00:00"],
    [1, "2023-10-01 15:00:00", "2023-10-01 23:00:00"],
    [1, "2023-10-01 16:00:00", "2023-10-02 00:00:00"],
    [2, "2023-10-01 09:00:00", "2023-10-01 17:00:00"],
    [2, "2023-10-01 11:00:00", "2023-10-01 19:00:00"],
    [3, "2023-10-01 09:00:00", "2023-10-01 17:00:00"],
]

employee_shifts = pd.DataFrame(
    employee_shifts_data,
    columns=["employee_id", "start_time", "end_time"]
).astype({
    "employee_id": "int64",
    "start_time": "datetime64[ns]",
    "end_time": "datetime64[ns]"
})


print(duckdb.query("""
/*
with cte as(
select e1.employee_id,sum(coalesce(datediff('Minute',e1.start_time,e2.end_time),0))
over(partition by  e1.employee_id) as total_overlap_duration from employee_shifts e1 left join employee_shifts e2 on e1.employee_id=e2.employee_id and e1.start_time <e2.end_time and e1.start_time >e2.start_time
),
max as(
select employee_id,max(over) as max_overlapping_shifts from (
select e1.employee_id,e2.start_time, count(*) as over
from employee_shifts e1 left join employee_shifts e2 on e1.employee_id=e2.employee_id and 
e1.start_time between e2.start_time and e2.end_time group by e1.employee_id,e2.start_time )
group by employee_id)

select distinct max.employee_id, max_overlapping_shifts,total_overlap_duration
from max left join cte on max.employee_id=cte.employee_id


*/

with cte as(

select e1.employee_id,e2.start_time, count(*) as over,
sum(case when e1.start_time<>e2.start_time then datediff('Minute',e1.start_time,e2.end_time) else 0 end)
/*sum(case when e1.start_time<>e2.start_time then timestampdiff())
sum(coalesce(datediff('Minute',e1.start_time,e2.end_time),0))
over(partition by  e1.employee_id) */
as total_overlap_duration
from employee_shifts e1 left join employee_shifts e2 on e1.employee_id=e2.employee_id and 
e1.start_time between e2.start_time and e2.end_time group by e1.employee_id,e2.start_time )

select employee_id,max(over) as max_overlapping_shifts, sum(total_overlap_duration) as total_overlap_duration from cte group by employee_id
""").to_df())