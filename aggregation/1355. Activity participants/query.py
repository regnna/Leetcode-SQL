import duckdb
import pandas as pd

friends_data = [
    [1, "Jonathan D.", "Eating"],
    [2, "Jade W.", "Singing"],
    [3, "Victor J.", "Singing"],
    [4, "Elvis Q.", "Eating"],
    [5, "Daniel A.", "Eating"],
    [6, "Bob B.", "Horse Riding"],
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
select activity,count(*) cnt from friends group by activity order by count(*)
),
cte2 as(
select id,a.name,coalesce(cnt,0) cnt
from activities a left join cte c on a.name=c.activity
)

select name as activity
from cte2 
where cnt <> (select max(cnt) from cte2) and cnt <>(Select min(cnt) from cte2) 
/*group by activity
having cnt<>max(cnt) and cnt<>min(cnt) */
""").to_df())