import pandas as pd
import duckdb


data=[
    [1,3.50],
    [2,3.65],
    [3,4.00],
    [4,3.85],
    [5,4.00],
    [6,3.65]
]

scores=pd.DataFrame(data,columns=['id','score']).astype(
    {"id":'int64',
    "score":'float64'
    })

print(duckdb.query("""
select score,dense_rank() over(order by score desc) as rank from scores
""").to_df())