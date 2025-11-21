import pandas as pd
import duckdb

data = [[1, 'Alice', 'Computer Science'], [2, 'Bob', 'Computer Science'], [3, 'Charlie', 'Mathematics'], [4, 'David', 'Mathematics']]
students = pd.DataFrame(data, columns=['student_id', 'name', 'major']).astype({
    'student_id': 'Int64',  # Nullable integer type for student_id
    'name': 'string',       # String type for names
    'major': 'string'       # String type for majors
})
data = [[101, 'Algorithms', 3, 'Computer Science', 'Yes'], [102, 'Data Structures', 3, 'Computer Science', 'Yes'], [103, 'Calculus', 4, 'Mathematics', 'Yes'], [104, 'Linear Algebra', 4, 'Mathematics', 'Yes'], [105, 'Machine Learning', 3, 'Computer Science', 'No'], [106, 'Probability', 3, 'Mathematics', 'No'], [107, 'Operating Systems', 3, 'Computer Science', 'No'], [108, 'Statistics', 3, 'Mathematics', 'No']]
courses = pd.DataFrame(data, columns=['course_id', 'name', 'credits', 'major', 'mandatory']).astype({'course_id': 'Int64', 'name': 'string', 'credits': 'Int64', 'major': 'string', 'mandatory': 'string'})

 # pd.CategoricalDtype(categories=['yes', 'no'])

data = [[1, 101, 'Fall 2023', 'A', 4.0], [1, 102, 'Spring 2023', 'A', 4.0], [1, 105, 'Spring 2023', 'A', 4.0], [1, 107, 'Fall 2023', 'B', 3.5], [2, 101, 'Fall 2023', 'A', 4.0], [2, 102, 'Spring 2023', 'B', 3.0], [3, 103, 'Fall 2023', 'A', 4.0], [3, 104, 'Spring 2023', 'A', 4.0], [3, 106, 'Spring 2023', 'A', 4.0], [3, 108, 'Fall 2023', 'B', 3.5], [4, 103, 'Fall 2023', 'B', 3.0], [4, 104, 'Spring 2023', 'B', 3.0]]
enrollments = pd.DataFrame(data, columns=['student_id', 'course_id', 'semester', 'grade', 'GPA']).astype({'student_id': 'Int64', 'course_id': 'Int64', 'semester': 'string', 'grade': 'string', 'GPA': 'float'})


print(duckdb.query("""

with cte as
(
select s.student_id
from students as s
left join courses as c
using (major)
left join enrollments as e
on s.student_id=e.student_id
and c.course_id=e.course_id
group by s.student_id
having sum(if(c.mandatory='Yes',1,0))=sum(if(c.mandatory='Yes',1,0)*if(e.grade='A',1,0))
and sum(if(c.mandatory='No',1,0)*if(e.grade in('A','B'),1,0))>=2
)

/*  Have taken all mandatory courses and at least two elective courses offered in their major.
    Achieved a grade of A in all mandatory courses and at least B in elective courses.
these two funtionalities has been implemented 

    Maintained an average GPA of at least 2.5 across all their courses (including those outside their major).
as it includes subject outside of their major we have to  do this -----
*/


select student_id
from cte
where student_id in 
    (select student_id from
    enrollments group by student_id having avg(GPA)>=2.5 )
order by student_id


""").to_df())