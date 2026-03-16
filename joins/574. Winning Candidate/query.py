import pandas as pd
import duckdb

candidate_data = [
    [1, 'A'],
    [2, 'B'],
    [3, 'C'],
    [4, 'D'],
    [5, 'E'],
]

candidate = pd.DataFrame(
    candidate_data,
    columns=['id', 'name']
).astype({
    'id': 'int64',
    'name': 'string'
})

vote_data = [
    [1, 2],
    [2, 4],
    [3, 3],
    [4, 2],
    [5, 5],
]

vote = pd.DataFrame(
    vote_data,
    columns=['id', 'candidateId']
).astype({
    'id': 'int64',
    'candidateId':'int64'
})

print(duckdb.query("""
select c.name from candidate c join
(select candidateId,count(candidateId) cnt from vote group by candidateId) d
on c.id=d.candidateId order by cnt desc limit 1

""").to_df())