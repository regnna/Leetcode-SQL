import pandas as pd
import duckdb

employees_data = [
    [1, 20],
    [2, 12],
    [3, 2]
]

employees = pd.DataFrame(
    employees_data,
    columns=["employee_id", "needed_hours"]
).astype({
    "employee_id": "int64",
    "needed_hours": "int64"
})

logs_data = [
    [1, "2022-10-01 09:00:00", "2022-10-01 17:00:00"],
    [1, "2022-10-06 09:05:04", "2022-10-06 17:09:03"],
    [1, "2022-10-12 23:00:00", "2022-10-13 03:00:01"],
    [2, "2022-10-29 12:00:00", "2022-10-29 23:58:58"]
]

logs = pd.DataFrame(
    logs_data,
    columns=["employee_id", "in_time", "out_time"]
).astype({
    "employee_id": "int64",
    "in_time": "datetime64[ns]",
    "out_time": "datetime64[ns]"
})

print(duckdb.query("""
select employee_id from (
select distinct e.employee_id,coalesce(sum(ceil(datediff('SECOND',in_time,out_time)/60)) over(partition by employee_id),0) as diff,
needed_hours*60 as Needed_time
/*date_trunc('minute',out_time-in_time)) over(partition by employee_id) as Diff */
from employees e left join logs  l using(employee_id))
where Needed_time>diff
""").to_df())