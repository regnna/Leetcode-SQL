import pandas as pd
import duckdb


friends_data = [
    [1, "Jonathan D.", "Eating"],
    [2, "Jade W.", "Singing"],
    [3, "Victor J.", "Singing"],
    [4, "Elvis Q.", "Eating"],
    [5, "Daniel A.", "Eating"],
    [6, "Bob B.", "Horse Riding"]
]

friends = pd.DataFrame(
    friends_data,
    columns=["id", "name", "activity"]
).astype({
    "id": "int64",
    "name": "string",
    "activity": "string"
})


activities_data = [
    [1, "Eating"],
    [2, "Singing"],
    [3, "Horse Riding"]
]

activities = pd.DataFrame(
    activities_data,
    columns=["id", "name"]
).astype({
    "id": "int64",
    "name": "string"
})

print(duckdb.query("""
with cte as(
select activity,count(id) as num from friends group by activity
),
cte2 as(
select activity,num,
MAX(num) OVER() AS max_val,
MIN(num) OVER() AS min_val
from cte  
)

select activity from cte2 where  num > min_val AND num < max_val;
/*select activity from cte where num !=(select max(num) from cte) and num!=(select min(num) from cte)*/

""").to_df())