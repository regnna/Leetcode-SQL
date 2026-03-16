import pandas as pd
import duckdb

data = [
    [1, 20, '2019-08-14'],
    [2, 50, '2019-08-14'],
    [1, 30, '2019-08-15'],
    [1, 35, '2019-08-16'],
    [2, 65, '2019-08-17'],
    [3, 20, '2019-08-18']
]

products = pd.DataFrame(
    data,
    columns=['product_id', 'new_price', 'change_date']
).astype({
    'product_id': 'int64',
    'new_price': 'int64',
    'change_date': 'datetime64[ns]'
})

print(duckdb.query("""
with cte as(
select product_id,new_price,change_date,max(change_date) over(partition by product_id ) as max_change_date from products 
),
cte2 as(
select * from cte where max_change_date=change_date
)

select distinct p.product_id,coalesce(c.new_price,10) as price
from cte2 c right join products p on c.product_id=p.product_id

""").to_df())