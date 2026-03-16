import pandas as pd
import duckdb

data=[
    [1,"Jack","M",1],
    [2,"Jane","F",1],
    [3,'Mark','M',2]
]

student=pd.DataFrame(data,columns=['student_id','student_name','gender','dept_id']).astype({
    'student_id':'int64',
    'student_name':'string',
    'gender':'string'
})

department=pd.DataFrame([[1,'Engineering'],
                         [2,'Science'],
                         [3,'Law']
                        ],columns=['dept_id','dept_name']).astype({
                            "dept_id":'int64',
                            'dept_name':'string'
                        })

print(duckdb.query("""
select dept_name,count(student_id) as student_number from department left join student using(dept_id) group by dept_name order by student_number desc,dept_name asc
""").to_df())