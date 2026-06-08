import duckdb
import pandas as pd

uservisits_data = [
    [1, "2020-11-28"],
    [1, "2020-10-20"],
    [1, "2020-12-03"],
    [2, "2020-10-05"],
    [2, "2020-12-09"],
    [3, "2020-11-11"],
]

uservisits = pd.DataFrame(
    uservisits_data,
    columns=["user_id", "visit_date"]
).astype({
    "user_id": "int64",
    "visit_date": "datetime64[ns]"
})

print(duckdb.query("""

select user_id,max(datediff('DAY',visit_date,nxt_visit)) diff from(
select *,coalesce(lead(visit_date) over(partition by user_id order by visit_date  ),'2021-01-01') as nxt_visit from uservisits
) group by user_id order by user_id

""").to_df())