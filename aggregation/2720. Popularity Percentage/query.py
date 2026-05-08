import pandas as pd
import duckdb


friends_data = [
    [2, 1],
    [1, 3],
    [4, 1],
    [1, 5],
    [1, 6],
    [2, 6],
    [7, 2],
    [8, 3],
    [3, 9],
]

friends = pd.DataFrame(
    friends_data,
    columns=["user1", "user2"]
).astype({
    "user1": "int64",
    "user2": "int64"
})

print(duckdb.query("""
with f as(
    select * from friends
    union 
    select user2, user1 from friends
),
c as(
select count(distinct user1) as cnt from f)

select user1,round(count(*)*100/(select cnt from c),2) as percentage_popularity from f group by user1 order by user1

/*select
count(distinct user) from
(select user1 as user from friends
union select user2 as user from friends)) as total
from friends*/

""").to_df())