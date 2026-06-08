import pandas as pd
import duckdb

student_data = [
    [1, "Daniel"],
    [2, "Jade"],
    [3, "Stella"],
    [4, "Jonathan"],
    [5, "Will"],
]

student = pd.DataFrame(
    student_data,
    columns=["student_id", "student_name"]
).astype({
    "student_id": "int64",
    "student_name": "string"
})

exam_data = [
    [10, 1, 70],
    [10, 2, 80],
    [10, 3, 90],
    [20, 1, 80],
    [30, 1, 70],
    [30, 3, 80],
    [30, 4, 90],
    [40, 1, 60],
    [40, 2, 70],
    [40, 4, 80],
]

exam = pd.DataFrame(
    exam_data,
    columns=["exam_id", "student_id", "score"]
).astype({
    "exam_id": "int64",
    "student_id": "int64",
    "score": "int64"
})


print(duckdb.query("""
with cte as(
select distinct student_id from (
select *,
rank() over(partition by exam_id order by score desc) as high_rnk,
rank() over(partition by exam_id order by score) as low_rnk 
from exam

) where high_rnk=1 or low_rnk=1
)


/*select * from cte*/

select * from student where student_id not in (select * from cte) and student_id in (select distinct student_id from exam) order by student_id
""").to_df())