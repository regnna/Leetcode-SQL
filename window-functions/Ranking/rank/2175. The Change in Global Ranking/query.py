import pandas as pd
import duckdb

team_points_data = [
    [3, 'Algeria', 1431],
    [1, 'Senegal', 2132],
    [2, 'New Zealand', 1402],
    [4, 'Croatia', 1817]
]

team_points = pd.DataFrame(
    team_points_data,
    columns=['team_id', 'name', 'points']
).astype({
    'team_id': 'int64',
    'name': 'string',
    'points': 'int64'
})

points_change_data = [
    [3, 399],
    [2, 0],
    [4, 13],
    [1, -22]
]

points_change = pd.DataFrame(
    points_change_data,
    columns=['team_id', 'points_change']
).astype({
    'team_id': 'int64',
    'points_change': 'int64'
})

print(duckdb.query("""
with cte as(
select tp.*,rank() over(order by points desc,name) as rnk,
tp.points+pc.points_change as new_points
from team_points tp left join  points_change pc on tp.team_id=pc.team_id
),

cte2 as(
select *,rank() over(order by new_points desc,name) as new_rnk from cte
)

select team_id,name,rnk-new_rnk as rank_diff from cte2



""").to_df())
