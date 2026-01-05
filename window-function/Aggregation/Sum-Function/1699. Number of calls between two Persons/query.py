import pandas as pd
import duckdb

data=[[1,2,59],[2,1,11],[1,3,20],[3,4,100],[3,4,200],[3,4,200],[4,3,499]]


"""
data = [
    [1, 2, 59],
    [2, 1, 11],
    [1, 3, 20],
    [3, 4, 100],
    [3, 4, 200],
    [3, 4, 200],
    [4, 3, 499]
]

calls=pd.DataFrame(data,columns=['from_id','to_id','duration']).astype({'from_id':'Int64'},{'to_id':'Int64'},{'duration':'Int64'})
"""
calls = pd.DataFrame(
    data,
    columns=['from_id', 'to_id', 'duration']
).astype({
    'from_id': 'Int64',
    'to_id': 'int64',
    'duration': 'int64'
})
print(duckdb.query("""
with cte as(select if(from_id>to_id,to_id,from_id) as from_id,if(to_id<from_id,from_id,to_id) as to_id,duration from calls)

/*select from_id,to_id, sum(duration) from cte group by from_id,to_id*/

select distinct from_id,to_id, sum(duration) over(partition by from_id,to_id)
 from cte


""").to_df())