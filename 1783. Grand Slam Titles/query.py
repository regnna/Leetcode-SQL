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
select year,"


"""))