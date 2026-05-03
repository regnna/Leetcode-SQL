import pandas as pd
import duckdb

enrollments_data = [
    [2, 2, 95],
    [2, 3, 95],
    [1, 1, 90],
    [1, 2, 99],
    [3, 1, 80],
    [3, 2, 75],
    [3, 3, 82],
]

enrollments = pd.DataFrame(
    enrollments_data,
    columns=["student_id", "course_id", "grade"]
).astype({
    "student_id": "int64",
    "course_id": "int64",
    "grade": "int64"
})

print(duckdb.query("""

select student_id,course_id,grade from(
select *,rank() over(partition by student_id order by grade desc,course_id ) as rnk from enrollments)
where rnk=1 order by student_id
""").to_df())