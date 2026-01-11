import pandas as pd
import duckdb



user_visits_data = [
    [1, "2020-11-28"],
    [1, "2020-10-20"],
    [1, "2020-12-03"],
    [2, "2020-10-05"],
    [2, "2020-12-09"],
    [3, "2020-11-11"]
]

user_visits = pd.DataFrame(
    user_visits_data,
    columns=["user_id", "visit_date"]
).astype({
    "user_id": "int64",
    "visit_date": "datetime64[ns]"
})

print(duckdb.query("""

with cte as(
select *,lead(visit_date) over(partition by user_id order by visit_date) as next_visit
from user_visits
),
cte2 as(
select user_id,case when next_visit is not null 
then datediff('day',visit_date,next_visit)
else datediff('day',visit_date,date('2021-1-1'))
end as allwindows
from cte
)

select user_id, max(allwindows) from cte2 group by user_id order by user_id


""").to_df())