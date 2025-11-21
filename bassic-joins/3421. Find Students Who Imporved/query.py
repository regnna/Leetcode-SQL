import pandas as pd
import duckdb


pd.set_option("display.max_columns",200)

data = [[101, 'Math', 70, '15-01-2023'], [101, 'Math', 85, '15-02-2023'], [101, 'Physics', 65, '15-01-2023'], [101, 'Physics', 60, '15-02-2023'], [102, 'Math', 80, '15-01-2023'], [102, 'Math', 85, '15-02-2023'], [103, 'Math', 90, '15-01-2023'], [104, 'Physics', 75, '15-01-2023'], [104, 'Physics', 85, '15-02-2023']]
scores = pd.DataFrame(data,columns=["student_id","subject","score","exam_date"]).astype({'student_id':'Int64','subject':'object','score':'Int64','exam_date':'object'})



print(duckdb.query("""
select s1.student_id,s1.subject,s1.score,s2.score
from scores as s1 left join scores s2
on s1.student_id=s2.student_id and s1.exam_date<s2.exam_date and s1.subject=s2.subject
where s2.score>s1.score
""").to_df())