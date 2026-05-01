import pandas as pd
import duckdb


students_data = [
    [1, "Alice Chen",   "Computer Science"],
    [2, "Bob Johnson",  "Mathematics"],
    [3, "Carol Davis",  "Physics"],
    [4, "David Wilson", "Chemistry"],
    [5, "Emma Brown",   "Biology"],
]

students = pd.DataFrame(
    students_data,
    columns=["student_id", "student_name", "major"]
).astype({
    "student_id": "int64",
    "student_name": "string",
    "major": "string"
})


study_sessions_data = [
    [1, 1, "Math",       "2023-10-01", 2.5],
    [2, 1, "Physics",    "2023-10-02", 3.0],
    [3, 1, "Chemistry",  "2023-10-03", 2.0],
    [4, 1, "Math",       "2023-10-04", 2.5],
    [5, 1, "Physics",    "2023-10-05", 3.0],
    [6, 1, "Chemistry",  "2023-10-06", 2.0],

    [7, 2, "Algebra",    "2023-10-01", 4.0],
    [8, 2, "Calculus",   "2023-10-02", 3.5],
    [9, 2, "Statistics", "2023-10-03", 2.5],
    [10,2, "Geometry",   "2023-10-04", 3.0],
    [11,2, "Algebra",    "2023-10-05", 4.0],
    [12,2, "Calculus",   "2023-10-06", 3.5],
    [13,2, "Statistics", "2023-10-07", 2.5],
    [14,2, "Geometry",   "2023-10-08", 3.0],

    [15,3, "Biology",    "2023-10-01", 2.0],
    [16,3, "Chemistry",  "2023-10-02", 2.5],
    [17,3, "Biology",    "2023-10-03", 2.0],
    [18,3, "Chemistry",  "2023-10-04", 2.5],

    [19,4, "Organic",    "2023-10-01", 3.0],
    [20,4, "Physical",   "2023-10-05", 2.5],
]

study_sessions = pd.DataFrame(
    study_sessions_data,
    columns=["session_id", "student_id", "subject", "session_date", "hours_studied"]
).astype({
    "session_id": "int64",
    "student_id": "int64",
    "subject": "string",
    "session_date": "datetime64[ns]",
    "hours_studied": "float64"
})


print(duckdb.query("""

/*
select distinct student_id,student_name,major,cycle_length,total_study_hours from
(select *,count(distinct subject)  over(partition by student_id) as cycle_length,sum(hours_studied) over(partition by student_id) as total_study_hours,
count(*) over(partition by student_id) as sessions from study_sessions)
as s left join students as stu using(student_id)
where cycle_length>=3 and (sessions/cycle_length)>=2
order by cycle_length desc,total_study_hours asc
*/

with cte as(
select student_id,subject,session_date,
lag(session_date) over(partition by student_id  order by session_date) as  prev_session,
sum(hours_studied) over(partition by student_id) as total_study_hours
from study_sessions),
cte2 as(
select student_id, max(datediff('day',prev_session,session_date)) max_diff 
from cte group by student_id
)
,
cte3 as(
select *, row_number() over(partition by student_id order by session_date) as rnk
 from (select * from cte c left join cte2 using(student_id) where max_diff<=2)
),
cte4 as(
select c1.*, (c2.rnk-c1.rnk) as sub_gap
from cte3 as c1 inner join cte3 c2 
    on c1.student_id=c2.student_id
    and c1.subject=c2.subject
    and c1.rnk<c2.rnk
),
list as(
 
select student_id,count(distinct subject) as cycle_length from cte4 group by student_id having count(distinct subject)>=3
)
/*select * from cte3*/

select distinct student_id,student_name,major,cycle_length,total_study_hours from list left join students s using(student_id) left join cte3 using(student_id) order by cycle_length desc,total_study_hours asc
""").to_df())