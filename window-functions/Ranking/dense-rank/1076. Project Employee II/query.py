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
    [3, "John", 1],
    [4, "Doe", 2]
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
/*with cte as(
select distinct project_id,count(employee_id) over(partition by project_id ) as numberOfEmp from project 
)

select * from cte

select project_id,DENSE_RANK() OVER (ORDER BY emp_count DESC) as rnk
    FROM cte
from project group by project_id */

/*
select project_id
from project 
group by project_id
having count(employee_id)=
(select count(employee_id) 
from project 
 group by project_id 
 order by count(employee_id) desc 
 limit 1)*/

WITH ProjectCounts AS (
    SELECT project_id, COUNT(employee_id) as emp_count
    FROM project
    GROUP BY project_id
),
RankedProjects AS (
    SELECT project_id, 
           DENSE_RANK() OVER (ORDER BY emp_count DESC) as rnk
    FROM ProjectCounts
)
SELECT *
FROM ProjectCounts 


""").to_df())