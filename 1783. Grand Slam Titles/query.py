import pandas as pd
import duckdb

players_data = [
    [1, 'Nadal'],
    [2, 'Federer'],
    [3, 'Novak']
]

players = pd.DataFrame(
    players_data,
    columns=['player_id', 'player_name']
).astype({
    'player_id': 'int64',
    'player_name': 'string'
})

championships_data = [
    [2018, 1, 1, 1, 1],
    [2019, 1, 1, 2, 2],
    [2020, 2, 1, 2, 2]
]

championships = pd.DataFrame(
    championships_data,
    columns=['year', 'Wimbledon', 'Fr_open', 'US_open', 'Au_open']
).astype({
    'year': 'int64',
    'Wimbledon': 'int64',
    'Fr_open': 'int64',
    'US_open': 'int64',
    'Au_open': 'int64'
})


print(duckdb.query("""
with cte as(
select year,\'Wimbledon\' as championship,Wimbledon as player_id from championships
union
select year,\'Fr_open\' as championship,Fr_open as player_id from championships
union
select year,\'US_open\' as championship,US_open as player_id from championships
union
select year,\'Au_open\' as championship,Au_open as player_id from championships
),
cte2 as
(select player_id,count(player_id) as grand_slam_count
from cte group by player_id
)

select c.player_id,p.player_name,c.grand_slam_count
from cte2 c left join 
players p using (player_id)
""").to_df())