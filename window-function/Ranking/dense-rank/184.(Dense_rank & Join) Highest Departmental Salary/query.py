import pandas as pd
import duckdb

employee_data = [
    [1, 'Joe',   70000, 1],
    [2, 'Jim',   90000, 1],
    [3, 'Henry', 80000, 2],
    [4, 'Sam',   60000, 2],
    [5, 'Max',   90000, 1],
]

employee = pd.DataFrame(
    employee_data,
    columns=['id', 'name', 'salary', 'departmentId']
).astype({
    'id': 'int64',
    'name': 'string',
    'salary': 'int64',
    'departmentId': 'int64'
})

department_data = [
    [1, 'IT'],
    [2, 'Sales'],
]

department = pd.DataFrame(
    department_data,
    columns=['id', 'name']
).astype({
    'id': 'int64',
    'name': 'string'
})

print(duckdb.query("""
select d.name as Department,e.name as employee,e.sa lary from(
select *, Dense_rank() over(partition by departmentId order by salary desc) as rnk from employee) e
left join department d on e.departmentId=d.id
where e.rnk=1
""").to_df())