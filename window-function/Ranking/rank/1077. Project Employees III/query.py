import pandas as pd
import duckdb

data=[
    [1,1],
    [1,2],
    [1,3],
    [2,1],
    [2,4]
]

project=pd.DataFrame(data,columns=['project_id','employee_id']).astype({
    'project_id':'int64',
    'employee_id':'int64'
})
employee_data = [
    [1, "Khaled", 3],
    [2, "Ali", 2],
    [3, "John", 3],
    [4, "Doe", 6]
]

employee = pd.DataFrame(
    employee_data,
    columns=["employee_id", "name", "experience_years"]
).astype({
    "employee_id": "int64",
    "name": "string",
    "experience_years": "int64"
})


print(duckdb.query("""
WITH RankedEmployees AS (
    SELECT 
        p.project_id, 
        p.employee_id,
        RANK() OVER(PARTITION BY p.project_id ORDER BY e.experience_years DESC) as rnk
    FROM project p
    JOIN employee e ON p.employee_id = e.employee_id
)

select * from RankedEmployees

/*
SELECT project_id, employee_id
FROM RankedEmployees
WHERE rnk = 1;

with cte as(
select * from project p left join employee e using (employee_id)
),
cte2 as(
select *,max(experience_years) over(partition by project_id) as maxinpro 
from cte
)


select 
project_id,employee_id from cte2 where experience_years=maxinpro


select * from cte2
select experience_years from employee  group by experience_years order by max(experience_years) desc limit 1*/

""").to_df())