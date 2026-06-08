import pandas as pd
import duckdb

course_completions_data = [
    [1, 101, "Python Basics",    "2024-01-05", 5],
    [1, 102, "SQL Fundamentals", "2024-02-10", 4],
    [1, 103, "JavaScript",       "2024-03-15", 5],
    [1, 104, "React Basics",     "2024-04-20", 4],
    [1, 105, "Node.js",          "2024-05-25", 5],
    [1, 106, "Docker",           "2024-06-30", 4],

    [2, 101, "Python Basics",    "2024-01-08", 4],
    [2, 104, "React Basics",     "2024-02-14", 5],
    [2, 105, "Node.js",          "2024-03-20", 4],
    [2, 106, "Docker",           "2024-04-25", 5],
    [2, 107, "AWS Fundamentals", "2024-05-30", 4],

    [3, 101, "Python Basics",    "2024-01-10", 3],
    [3, 102, "SQL Fundamentals", "2024-02-12", 3],
    [3, 103, "JavaScript",       "2024-03-18", 3],
    [3, 104, "React Basics",     "2024-04-22", 2],
    [3, 105, "Node.js",          "2024-05-28", 3],

    [4, 101, "Python Basics",    "2024-01-12", 5],
    [4, 108, "Data Science",     "2024-02-16", 5],
    [4, 109, "Machine Learning", "2024-03-22", 5],
]

course_completions = pd.DataFrame(
    course_completions_data,
    columns=[
        "user_id",
        "course_id",
        "course_name",
        "completion_date",
        "course_rating"
    ]
).astype({
    "user_id": "int64",
    "course_id": "int64",
    "course_name": "string",
    "completion_date": "datetime64[ns]",
    "course_rating": "int64"
})


print(duckdb.query("""
with cte as(
select user_id from course_completions group by user_id having count(distinct course_id) >=5 and avg(course_rating)>=4
),
course_info as
(select * ,lead(course_name,1) over(partition by user_id order by completion_date) as second_course from course_completions where user_id in (select * from cte)
)

select course_name as first_course,second_course,count(user_id) as transition_count from course_info where second_course <> 'NULL' group by first_course,second_course order by transition_count desc,first_course,second_course
""").to_df())
