import pandas as pd
import duckdb

import pandas as pd

employees = pd.DataFrame(
    [
        [2, 'Meir', 3000],
        [3, 'Michael', 3000],
        [7, 'Addilyn', 7400],
        [8, 'Juan', 6100],
        [9, 'Kannon', 7400],
        
    ],
    columns=['employee_id', 'name', 'salary']
).astype({
    'employee_id': 'int64',
    'name': 'string',
    'salary': 'int64'
})

print(duckdb.query("""
/*with cte as(
select *,dense_rank() over(order by salary ) as team_id  from employees 
),
c2 as(
select   salary,count(salary) as sal from cte group by salary
)

select employee_id,name, t.salary, team_id from cte t left join c2 on t.salary=c2.salary where sal>1 order by team_id,employee_id*/

SELECT 
    employee_id,
    name,
    salary,
    DENSE_RANK() OVER (ORDER BY salary) as team_id
FROM (
    SELECT *, COUNT(*) OVER (PARTITION BY salary) as cnt
    FROM employees
) t
WHERE cnt >= 2
ORDER BY team_id, employee_id;



""").to_df())
