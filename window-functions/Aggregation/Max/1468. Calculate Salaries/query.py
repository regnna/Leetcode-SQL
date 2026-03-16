import pandas as pd
import duckdb


data = [
    [1, 1, 'Tony', 2000],
    [1, 2, 'Pronub', 21300],
    [1, 3, 'Tyrrox', 10800],
    [2, 1, 'Pam', 300],
    [2, 7, 'Bassem', 450],
    [2, 9, 'Hermione', 700],
    [3, 7, 'Bocaben', 100],
    [3, 2, 'Ognjen', 2200],
    [3, 13, 'Nyancat', 3300],
    [3, 15, 'Morninngcat', 7777]
]

salaries = pd.DataFrame(
    data,
    columns=['company_id', 'employee_id', 'employee_name', 'salary']
).astype({
    'company_id': 'int64',
    'employee_id': 'int64',
    'employee_name': 'string',
    'salary': 'int64'
})

print(duckdb.query("""
with cte as(
select company_id,employee_id,employee_name,salary,max(salary) over(partition by company_id) as mx from salaries
)
select company_id,employee_id,employee_name, cast(case 
when mx <1000 then salary
when mx>=1000 and mx<=10000 then 0.76*salary
when mx>10000 then 0.51*salary 
end as int) as salary
 from cte 
""").to_df())