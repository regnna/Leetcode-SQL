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
    'project_id':'int',
    'employee_id':'int'
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

select project_id,
/*round(sum(experience_years)/sum(count(*)) over(partition by project_id),2) as Average_years*/
round(avg(experience_years),2) as Average_years
from project p inner join employee e using(employee_id) group by project_id

""").to_df())