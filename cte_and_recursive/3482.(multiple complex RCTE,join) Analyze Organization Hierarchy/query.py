import pandas as pd
import duckdb

employees_data = [
    [1,  "Alice",   None, 12000, "Executive"],
    [2,  "Bob",     1,    10000, "Sales"],
    [3,  "Charlie", 1,    10000, "Engineering"],
    [4,  "David",   2,     7500, "Sales"],
    [5,  "Eva",     2,     7500, "Sales"],
    [6,  "Frank",   3,     9000, "Engineering"],
    [7,  "Grace",   3,     8500, "Engineering"],
    [8,  "Hank",    4,     6000, "Sales"],
    [9,  "Ivy",     6,     7000, "Engineering"],
    [10, "Judy",    6,     7000, "Engineering"]
]

employees = pd.DataFrame(
    employees_data,
    columns=[
        "employee_id",
        "employee_name",
        "manager_id",
        "salary",
        "department"
    ]
)

employees = employees.astype({
    "employee_id": "int64",
    "employee_name": "string",
    "salary": "int64",
    "department": "string"
})


print(duckdb.query("""
 
 with recursive cte as
 (
    select employee_id,employee_name,manager_id,1 as Level
    from employees
    where manager_id is null

    union

    select e.employee_id,e.employee_name,e.manager_id,c.level+1 from 
    employees e inner join
    cte c on e.manager_id=c.employee_id

)
,cte2 as(
    select employee_id,employee_id as manager_id from employees

    union
    
    select e.employee_id,c2.manager_id
    from cte2 c2 join employees  e on c2.employee_id=e.manager_id
),
cte3 as(
select c1.employee_id,c1.employee_name,c1.level,c2.employee_id as eid,c2.manager_id,e.salary from cte c1 
inner join cte2 c2 on c1.employee_id=c2.manager_id
inner join employees e on c2.employee_id=e.employee_id)

select employee_id,employee_name,level,count(distinct case when employee_id<>eid then eid else null end) as team_size,sum(salary) as budget
from cte3 
group by employee_id,employee_name,level
order by level,budget desc,employee_name
""").to_df())