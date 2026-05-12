import pandas as pd
import duckdb

salary_data = [
    [1, 1, 9000, "2017-03-31"],
    [2, 2, 6000, "2017-03-31"],
    [3, 3, 10000, "2017-03-31"],
    [4, 1, 7000, "2017-02-28"],
    [5, 2, 6000, "2017-02-28"],
    [6, 3, 8000, "2017-02-28"],
]

salary = pd.DataFrame(
    salary_data,
    columns=["id", "employee_id", "amount", "pay_date"]
).astype({
    "id": "int64",
    "employee_id": "int64",
    "amount": "int64",
    "pay_date": "datetime64[ns]"
})

employee_data = [
    [1, 1],
    [2, 2],
    [3, 2],
]

employee = pd.DataFrame(
    employee_data,
    columns=["employee_id", "department_id"]
).astype({
    "employee_id": "int64",
    "department_id": "int64"
})


print(duckdb.query("""
with cte as(
select *,strftime(pay_date,'%Y-%m') as m, from salary s left join employee e using(employee_id) 
),
cte2 as(
select department_id,avg(amount) as dept_avg,m from cte group by m,department_id
),
cte3 as(
select m,avg(amount) as comp_avg from cte group by m
)
select m as pay_month, c2.department_id,
case when dept_avg>comp_avg then 'higher'
    when dept_avg<comp_avg then 'lower'
    else 'same' end as comparison 

  from cte2 as c2 left  join cte3 as c3 using(m)



""").to_df())