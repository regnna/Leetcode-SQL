import pandas as pd
import duckdb


data=[
    [1,'Abbot'],
    [2,'Doris'],
    [3,'Emerson'],
    [4,'Green'],
    [5,'Jeames']
]

seat=pd.DataFrame(data,columns=['id','student']).astype(
    {'id':'int64'},
    {'student':'string'})


print(duckdb.query("""

with cte as(
select id,student,
 /*CASE 
        WHEN mod(id, 2) <> 0 THEN COALESCE(LEAD(student) OVER(ORDER BY id), student)
        ELSE LAG(student) OVER(ORDER BY id)*/

case when mod(id,2)=0 then id-1 
                    /*when id=(select max(id) from seat)*/
                    when COUNT(*) OVER()
                     and mod(id,2)<>0 then id
                    else id+1 end as idd from seat

)

-- Only works in specific DBs like DuckDB or Snowflake
/*SELECT student
FROM Seat
QUALIFY COUNT(*) OVER() > 1;*/

select idd as id, student,(select max(id) from seat) maaax,count(*) over()
from cte order by idd
""").to_df())