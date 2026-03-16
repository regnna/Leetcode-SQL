import pandas as pd
import duckdb

data = [
    [2, 2, 95],
    [2, 3, 95],
    [1, 1, 90],
    [1, 2, 99],
    [3, 1, 80],
    [3, 2, 75],
    [3, 3, 82]
]

enrollments = pd.DataFrame(data,columns=['student_id','course_id','grade']).astype(
    {'student_id':'Int64',
    'course_id':"Int64",
    'grade':"int64" 
    })


print(duckdb.query("""
with cte as(
select *, rank() over(partition by student_id order by grade desc,course_id asc ) as graaade
from enrollments order by student_id
)

select student_id,course_id,grade from cte where graaade=1


""").to_df())