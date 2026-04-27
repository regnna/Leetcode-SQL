import pandas as pd
import duckdb

employee_data = [
    [1, "Joe",   85000, 1],
    [2, "Henry", 80000, 2],
    [3, "Sam",   60000, 2],
    [4, "Max",   90000, 1],
    [5, "Janet", 69000, 1],
    [6, "Randy", 85000, 1],
    [7, "Will",  70000, 1],
]

employee = pd.DataFrame(
    employee_data,
    columns=["id", "name", "salary", "departmentId"]
).astype({
    "id": "int64",
    "name": "string",
    "salary": "int64",
    "departmentId": "int64"
})

department_data = [
    [1, "IT"],
    [2, "Sales"],
]

department = pd.DataFrame(
    department_data,
    columns=["id", "name"]
).astype({
    "id": "int64",
    "name": "string"
})

print(duckdb.query("""
select Department,name as Employee, salary from (
select e.*,d.name as Department,dense_rank() over(partition by departmentId order by salary desc) as rnk ,  from employee e left join department d on e.departmentId=d.id order by departmentID,salary desc
)
where rnk <=3
""").to_df())