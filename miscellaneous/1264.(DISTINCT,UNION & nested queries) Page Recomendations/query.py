import pandas as pd
import duckdb

friendship=pd.DataFrame([
    [1,2],
    [1,3],
    [1,4],
    [2,3],
    [2,4],
    [2,5],
    [6,1]
],columns=['user1_id','user2_id']).astype({
    "user1_id":"int64",
    "user2_id":"int64"
})

likes=pd.DataFrame([
    [1,88],
    [2,23],
    [3,24],
    [4,56],
    [5,11],
    [6,33],
    [2,77],
    [3,77],
    [6,88]
],columns=['user_id','page_id']).astype({
    'user_id':'int64',
    'page_id':'int64'
})


print(duckdb.query("""
select distinct page_id as recommended_page from likes where user_id in (
Select user2_id from friendship where user1_id=1
union select user1_id from friendship where user2_id=1) and page_id not in 
(select page_id from likes where user_id=1)
""").to_df())