import pandas as pd
import duckdb

genders_data = [
    [4, 'male'],
    [7, 'female'],
    [2, 'other'],
    [5, 'male'],
    [3, 'female'],
    [8, 'male'],
    [6, 'other'],
    [1, 'other'],
    [9, 'female']
]

genders = pd.DataFrame(
    genders_data,
    columns=['user_id', 'gender']
).astype({
    'user_id': 'int64',
    'gender': 'string'
})

print(duckdb.query("""
/*select * from genders order by user_id*/
with t as(
select *, rank() over(partition by gender order by user_id) as rnk1,
case 
    when gender='female' then 0
    when gender='other' then 1
    else 2
    end as rnk2
 from genders
)

select user_id, gender from t order by rnk1,rnk2 



""").to_df())