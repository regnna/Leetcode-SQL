import pandas as pd
import duckdb

data=pd.DataFrame([
    [4,2],
    [2,3],
    [3,2],
    [1,4]
],columns=['first_col','second_col']).astype({
    'first_col':'int64',
    'second_col':'int64'
})

print(duckdb.query("""
with t as(
select first_col, row_number() over(order by first_col ) as rn from data
),
s as(
select second_col, row_number() over(order by second_col desc ) as rn from data )


select first_col,second_col from t join s using(rn)
""").to_df())