import pandas as pd
import duckdb

contests_data = [
    [190, 1, 5, 2],
    [191, 2, 3, 5],
    [192, 5, 2, 3],
    [193, 1, 3, 5],
    [194, 4, 5, 2],
    [195, 4, 2, 1],
    [196, 1, 5, 2],
]

contests = pd.DataFrame(
    contests_data,
    columns=[
        "contest_id",
        "gold_medal",
        "silver_medal",
        "bronze_medal"
    ]
).astype({
    "contest_id": "int64",
    "gold_medal": "int64",
    "silver_medal": "int64",
    "bronze_medal": "int64"
})

users_data = [
    [1, "sarah@leetcode.com", "Sarah"],
    [2, "bob@leetcode.com", "Bob"],
    [3, "alice@leetcode.com", "Alice"],
    [4, "hercy@leetcode.com", "Hercy"],
    [5, "quarz@leetcode.com", "Quarz"],
]

users = pd.DataFrame(
    users_data,
    columns=["user_id", "mail", "name"]
).astype({
    "user_id": "int64",
    "mail": "string",
    "name": "string"
})


print(duckdb.query("""
/*select name,mail from users u left join 
(select gold_medal,count(contest_id) gold from contests group by gold_medal) as c on gold_medal=user_id where c.gold>=3
union
select name, mail from users u left join */

with cte as(
select gold_medal as user_id, contest_id from contests
union
select silver_medal as user_id, contest_id from contests
union
select bronze_medal as user_id, contest_id from contests
),
cte2 as(
select user_id,contest_id,row_number() over(partition by user_id order by contest_id) as rnk from cte order by user_id,rnk
)
,cte3 as(
select distinct user_id from cte2 group by user_id,contest_id-rnk having count(*)>=3
union
select gold_medal as user_id from contests group by gold_medal having count(gold_medal)>=3
)

select u.name,u.mail from cte3 c left join users u using(user_id)

""").to_df())