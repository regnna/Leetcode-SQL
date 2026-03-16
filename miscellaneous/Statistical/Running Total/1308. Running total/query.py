import pandas as pd
import duckdb

# Scores table data
data = [
    ['Aron',     'F', '2020-01-01', 17],
    ['Alice',    'F', '2020-01-07', 23],
    ['Bajrang',  'M', '2020-01-07', 7],
    ['Khali',    'M', '2019-12-25', 11],
    ['Slaman',   'M', '2019-12-30', 13],
    ['Joe',      'M', '2019-12-31', 3],
    ['Jose',     'M', '2019-12-18', 2],
    ['Priya',    'F', '2019-12-31', 23],
    ['Priyanka', 'F', '2019-12-30', 17]
]

scores = pd.DataFrame(
    data,
    columns=['player_name', 'gender', 'day', 'score_points']
).astype({
    'player_name': 'string',
    'gender': 'string',
    'day': 'datetime64[ns]',
    'score_points': 'int64'
})

print(duckdb.query("""
select gender, day, sum(score_points) over(partition by gender order by gender,day) as total
from scores

""").to_df())