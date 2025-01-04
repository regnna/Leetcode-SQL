import pandas as pd
import duckdb

data = [[1, 'Alice', 'Computer Science'], [2, 'Bob', 'Computer Science'], [3, 'Charlie', 'Mathematics'], [4, 'David', 'Mathematics']]
students = pd.DataFrame(data, columns=['student_id', 'name', 'major']).astype({
    'student_id': 'Int64',  # Nullable integer type
    'name': 'object',       # Object type for arbitrary strings, equivalent to VARCHAR
    'major': 'object'       # Object type for arbitrary strings, equivalent to VARCHAR
})
data = [[101, 'Algorithms', 3, 'Computer Science'], [102, 'Data Structures', 3, 'Computer Science'], [103, 'Calculus', 4, 'Mathematics'], [104, 'Linear Algebra', 4, 'Mathematics']]
courses = pd.DataFrame(data, columns=['course_id', 'name', 'credits', 'major']).astype({
    'course_id': 'Int64',  # Nullable integer type
    'name': 'object',      # Object type for arbitrary strings, equivalent to VARCHAR
    'credits': 'Int64',    # Nullable integer type
    'major': 'object'      # Object type for arbitrary strings, equivalent to VARCHAR
})
data = [[1, 101, 'Fall 2023', 'A'], [1, 102, 'Fall 2023', 'A'], [2, 101, 'Fall 2023', 'B'], [2, 102, 'Fall 2023', 'A'], [3, 103, 'Fall 2023', 'A'], [3, 104, 'Fall 2023', 'A'], [4, 103, 'Fall 2023', 'A'], [4, 104, 'Fall 2023', 'B']]
enrollments = pd.DataFrame(data, columns=['student_id', 'course_id', 'semester', 'grade']).astype({
    'student_id': 'Int64',  # Nullable integer type
    'course_id': 'Int64',   # Nullable integer type
    'semester': 'object',   # Object type for arbitrary strings
    'grade': 'object'       # Object type for arbitrary strings
})

print(duckdb.query('''
    select st.student_id
    from students as st 
    left join courses as c 
    using (major) 
    left join enrollments as e
    on st.student_id = e.student_id 
    and c.course_id = e.course_id
    group by st.student_id
    having count(distinct c.course_id) = Sum(if(e.grade='A',1,0))
    order by st.student_id
    ;

''').to_df())