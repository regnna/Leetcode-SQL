import pandas as pd
import duckdb

user_activity_data = [
    ["Alice", "Travel",  "2020-02-12", "2020-02-20"],
    ["Alice", "Dancing", "2020-02-21", "2020-02-23"],
    ["Alice", "Travel",  "2020-02-24", "2020-02-28"],
    ["Bob",   "Travel",  "2020-02-11", "2020-02-18"],
]

user_activity = pd.DataFrame(
    user_activity_data,
    columns=["username", "activity", "startDate", "endDate"]
).astype({
    "username": "string",
    "activity": "string",
    "startDate": "datetime64[ns]",
    "endDate": "datetime64[ns]"
})

print(duckdb.query("""

with cte as (
select distinct * from user_activity
)
select  username,activity,startDate,endDate from(
select *, rank() over(partition by username order by startDate desc) as rnk from cte)
where rnk =2
union 
select * from user_activity where username in
(select username from user_activity group by username having count(username)=1)
""").to_df())