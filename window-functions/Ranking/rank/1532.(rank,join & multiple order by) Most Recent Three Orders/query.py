import pandas as pd
import duckdb


customers_data = [
    [1, 'Winston'],
    [2, 'Jonathan'],
    [3, 'Annabelle'],
    [4, 'Marwan'],
    [5, 'Khaled']
]

customers = pd.DataFrame(
    customers_data,
    columns=['customer_id', 'name']
).astype({
    'customer_id': 'int64',
    'name': 'string'
})


orders_data = [
    [1,  '2020-07-31', 1, 30],
    [2,  '2020-07-30', 2, 40],
    [3,  '2020-07-31', 3, 70],
    [4,  '2020-07-29', 4, 100],
    [5,  '2020-06-10', 1, 1010],
    [6,  '2020-08-01', 2, 102],
    [7,  '2020-08-01', 3, 111],
    [8,  '2020-08-03', 1, 99],
    [9,  '2020-08-07', 2, 32],
    [10, '2020-07-15', 1, 2]
]

orders = pd.DataFrame(
    orders_data,
    columns=['order_id', 'order_date', 'customer_id', 'cost']
).astype({
    'order_id': 'int64',
    'order_date': 'datetime64[ns]',
    'customer_id': 'int64',
    'cost': 'int64'
})


print(duckdb.query("""
with cte as(
select c.customer_id,c.name as customer_name,o.order_id,o.order_date,
rank() over(partition by c.customer_id order by order_date desc ) as rnk
from orders o left join customers c using(customer_id) 
)


select customer_name,customer_id,order_id, order_date
from cte where rnk<=3 order by customer_name,customer_id,order_date desc

""").to_df())