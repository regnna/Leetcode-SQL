import pandas as pd
import duckdb

users_data = [
    [1, "Moustafa", 100],
    [2, "Jonathan", 200],
    [3, "Winston", 10000],
    [4, "Luis", 800],
]

users = pd.DataFrame(
    users_data,
    columns=["user_id", "user_name", "credit"]
).astype({
    "user_id": "int64",
    "user_name": "string",
    "credit": "int64"
})


transactions_data = [
    [1, 1, 3, 400, "2020-08-01"],
    [2, 3, 2, 500, "2020-08-02"],
    [3, 2, 1, 200, "2020-08-03"],
]

transactions = pd.DataFrame(
    transactions_data,
    columns=["trans_id", "paid_by", "paid_to", "amount", "transacted_on"]
).astype({
    "trans_id": "int64",
    "paid_by": "int64",
    "paid_to": "int64",
    "amount": "int64",
    "transacted_on": "datetime64[ns]"
})

print(duckdb.query("""
with cte as(

select paid_to as user_id, sum(amount) pveBal from transactions group by paid_to
),
cte2 as(
select paid_by as user_id,sum(amount) nveBal from transactions group by paid_by
)
select *, case when credit<0 then 'yes' else 'No' end as credit_limit_breached from(
select u.user_id,u.user_name,coalesce(credit-nveBal+pveBal,credit) as credit

 from users u left join cte c using(user_id) left join cte2 using(user_id))
""").to_df())