import pandas as pd
import duckdb

candidates_data_1 = [
    [1,  "Junior", 10000],
    [9,  "Junior", 10000],
    [2,  "Senior", 20000],
    [11, "Senior", 20000],
    [13, "Senior", 50000],
    [4,  "Junior", 40000],
]

candidates = pd.DataFrame(
    candidates_data_1,
    columns=["employee_id", "experience", "salary"]
).astype({
    "employee_id": "int64",
    "experience": "string",
    "salary": "int64"
})

candidates_data_2 = [
    [1,  "Junior", 10000],
    [9,  "Junior", 10000],
    [2,  "Senior", 80000],
    [11, "Senior", 80000],
    [13, "Senior", 80000],
    [4,  "Junior", 40000],
]

candidates_2 = pd.DataFrame(
    candidates_data_2,
    columns=["employee_id", "experience", "salary"]
).astype({
    "employee_id": "int64",
    "experience": "string",
    "salary": "int64"
})



print(duckdb.query("""
with cte as (
select *,sum(salary) over(partition by experience order by salary rows between unbounded preceding and current row ) as total_sal  from candidates_2
)

select 'Senior' as experience , count(employee_id) as accepted_candidates
from cte where total_sal<=70000 and experience='Senior'
union
select 'Junior' as experience, count(employee_id) as accepted_candidates
from cte where total_sal<=70000-ifnull((select max(total_sal) from cte where total_sal<=70000 and experience='Senior'),0) and experience='Junior'
""").to_df())