import pandas as pd
import duckdb


data=[
    [1,100],
    [2,200],
    [3,300],
    [4,10],
    [5,500],
    [6,400]
]
data1=[[1,100],[2,100]]

employee=pd.DataFrame(data1,columns=['id','salary']).astype({'id':'int64','salary':'int64'})


print(duckdb.query("""

with cte as(
select id,salary,dense_rank() over(order by salary desc) as rnk from employee
)

select max(salary) as SecondHighestSalary from cte where rnk=2

/*
SELECT (
    SELECT DISTINCT salary 
    FROM Employee 
    ORDER BY salary DESC 
    LIMIT 1 OFFSET 1
) AS SecondHighestSalary;
*/
""").to_df())