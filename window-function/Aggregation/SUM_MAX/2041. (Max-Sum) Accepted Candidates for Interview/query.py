import pandas as pd
import duckdb

candidates_data = [
    [11, 'Atticus', 1, 101],
    [9,  'Ruben',   6, 104],
    [6,  'Aliza',   10, 109],
    [8,  'Alfredo', 0, 107]
]

candidates = pd.DataFrame(
    candidates_data,
    columns=['candidate_id', 'name', 'years_of_exp', 'interview_id']
).astype({
    'candidate_id': 'int64',
    'name': 'string',
    'years_of_exp': 'int64',
    'interview_id': 'int64'
})


rounds_data = [
    [109, 3, 4],
    [101, 2, 8],
    [109, 4, 1],
    [107, 1, 3],
    [104, 3, 6],
    [109, 1, 4],
    [104, 4, 7],
    [104, 1, 2],
    [109, 2, 1],
    [104, 2, 7],
    [107, 2, 3],
    [101, 1, 8]
]

rounds = pd.DataFrame(
    rounds_data,
    columns=['interview_id', 'round_id', 'score']
).astype({
    'interview_id': 'int64',
    'round_id': 'int64',
    'score': 'int64'
})


print(duckdb.query("""

with cte as(

select distinct interview_id, max(round_id) over(partition by interview_id) as mx, sum(score) over(partition by interview_id) as sm from rounds )

select can.candidate_id from cte c left join candidates can using(interview_id) where can.years_of_exp>=2 and c.sm >15

""").to_df())