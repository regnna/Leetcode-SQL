import pandas as pd
import duckdb

employee_data = [
    [1,  "A", 2341],
    [2,  "A", 341],
    [3,  "A", 15],
    [4,  "A", 15314],
    [5,  "A", 451],
    [6,  "A", 513],
    [7,  "B", 15],
    [8,  "B", 13],
    [9,  "B", 1154],
    [10, "B", 1345],
    [11, "B", 1221],
    [12, "B", 234],
    [13, "C", 2345],
    [14, "C", 2645],
    [15, "C", 2645],
    [16, "C", 2652],
    [17, "C", 65],
]

employee = pd.DataFrame(
    employee_data,
    columns=["id", "company", "salary"]
).astype({
    "id": "int64",
    "company": "string",
    "salary": "int64"
})

print(duckdb.query("""
with cte as(
select company,case when count(company)%2=0 then 1 
else 0 end as odd,count(company) cnt
from employee group by company
),
cte2 as(
select id,e.company,salary, row_number() over(partition by company order by salary) rnk,c.odd,c.cnt
from employee e left join cte c using(company)
)

select distinct id, company, salary from cte2 where 
rnk between cnt/2 and (cnt/2)+1 
/*(odd=1 and (rnk=cnt/2 or rnk=cnt/2+1) ) or
(odd=0 and rnk=(cnt+1)/2 )*/

""").to_df())