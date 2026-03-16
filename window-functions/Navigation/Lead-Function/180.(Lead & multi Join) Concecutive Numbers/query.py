import pandas as pd
import duckdb

logs=pd.DataFrame([
    [1,1],
    [2,1],
    [3,1],
    [4,2],
    [5,1],
    [6,2],
    [7,2],
    [8,2]

],columns=['id','num']).astype({
    'id':'int64',
    'num':'int64'
})

print(duckdb.query("""
/*select distinct num as ConsecutiveNums  from(
select num,lead(num,1) over() nxt_1,lead(num,2) over() nxt_2  from logs
)
where num=nxt_1 and num=nxt_2 

SELECT DISTINCT l2.num AS ConsecutiveNums
FROM
    logs AS l1
    JOIN logs AS l2 ON l1.id = l2.id - 1 AND l1.num = l2.num
    JOIN logs AS l3 ON l2.id = l3.id - 1 AND l2.num=l3.num
*/
select num as ConsecutiveNums from(
select num,lead(num,1) over() as nxt,lead(num,2) over() as nxt2 from logs)
where num=nxt and num=nxt2
""").to_df())