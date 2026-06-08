import pandas as pd
import duckdb

visits_data = [
    [1,  "2020-01-01"],
    [2,  "2020-01-02"],
    [12, "2020-01-01"],
    [19, "2020-01-03"],
    [1,  "2020-01-02"],
    [2,  "2020-01-03"],
    [1,  "2020-01-04"],
    [7,  "2020-01-11"],
    [9,  "2020-01-25"],
    [8,  "2020-01-28"],
]

visits = pd.DataFrame(
    visits_data,
    columns=["user_id", "visit_date"]
).astype({
    "user_id": "int64",
    "visit_date": "datetime64[ns]"
})

transactions_data=[
    [1, "2020-01-02", 120],
    [2, "2020-01-03", 22],
    [7, "2020-01-11", 232],
    [1, "2020-01-04", 7],
    [9, "2020-01-25", 33],
    [9, "2020-01-25", 66],
    [9, "2020-01-25", 99],
    [8, "2020-01-28", 1],
]

transactions = pd.DataFrame(
    transactions_data,
    columns=["user_id", "transaction_date", "amount"]
).astype({
    "user_id": "int64",
    "transaction_date": "datetime64[ns]",
    "amount": "int64"
}) 

print(duckdb.query("""
with recursive c as(
select v.visit_date,v.user_id,sum(case when t.amount is null then 0 else 1 end) as num_tran
from visits as v
left join transactions as t 
on v.user_id=t.user_id
and v.visit_date=t.transaction_date group by 
v.visit_date, v.user_id
),

 c2 as(
select 0 as num_tran

union
select num_tran+1
from c2

where num_tran<(select Max(num_tran) from c)
)

select c2.num_tran as transaction_count,count(user_id) as visits_count from c2 
left join c using(num_tran) group by c2.num_tran
order by transaction_count
""").to_df())