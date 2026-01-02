import pandas as pd
import duckdb 

employees_data = [
    [1,  'Boss',   1],
    [3,  'Alice',  3],
    [2,  'Bob',    1],
    [4,  'Daniel', 2],
    [7,  'Luis',   4],
    [8,  'Jhon',   3],
    [9,  'Angela', 8],
    [77, 'Robert', 1]
]

employees = pd.DataFrame(
    employees_data,
    columns=['employee_id', 'employee_name', 'manager_id']
).astype({
    'employee_id': 'int64',
    'employee_name': 'string',
    'manager_id': 'int64'
})

print(duckdb.query("""
with Recursive cte as(
select employee_id from employees where manager_id=1 and employee_id<>1

union all

select e.employee_id  from employees e join cte c on  e.manager_id=c.employee_id

)

select distinct(employee_id) from cte

""").to_df())