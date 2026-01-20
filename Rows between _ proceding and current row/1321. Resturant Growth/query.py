import pandas as pd
import duckdb

data = [
    [1, 'Jhon',    '2019-01-01', 100],
    [2, 'Daniel',  '2019-01-02', 110],
    [3, 'Jade',    '2019-01-03', 120],
    [4, 'Khaled',  '2019-01-04', 130],
    [5, 'Winston', '2019-01-05', 110],
    [6, 'Elvis',   '2019-01-06', 140],
    [7, 'Anna',    '2019-01-07', 150],
    [8, 'Maria',   '2019-01-08', 80],
    [9, 'Jaze',    '2019-01-09', 110],
    [1, 'Jhon',    '2019-01-10', 130],
    [3, 'Jade',    '2019-01-10', 150],
]

customer = pd.DataFrame(
    data,
    columns=['customer_id', 'name', 'visited_on', 'amount']
).astype({
    'customer_id': 'int64',
    'name': 'string',
    'visited_on': 'datetime64[ns]',
    'amount': 'int64'
})

print(duckdb.query("""
with cte as(
select visited_on,sum(amount) as total_amount from customer group by visited_on order by visited_on
),
cte2 as(
select visited_on,
    sum(total_amount) over(order by visited_on rows BETWEEN 6 PRECEDING AND CURRENT ROW) as amount,
    round(avg(total_amount) over(order by visited_on rows between 6 preceding and current row),2) as average_amount,
    count(*) over (
            order by visited_on
            rows between 6 preceding and current row
        ) as window_size
from cte
)
 select visited_on, amount,average_amount from cte2 
 where window_size>=7
 
 
 /*where 
 visited_on>=
 (select visited_on from cte2 order by visited_on limit 1)+6*/


""").to_df())
