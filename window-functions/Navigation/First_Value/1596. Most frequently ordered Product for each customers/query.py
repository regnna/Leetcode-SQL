import pandas as pd
import duckdb

customers_data = [
    [1, 'Alice'],
    [2, 'Bob'],
    [3, 'Tom'],
    [4, 'Jerry'],
    [5, 'John']
]

customers = pd.DataFrame(
    customers_data,
    columns=['customer_id', 'name']
).astype({
    'customer_id': 'int64',
    'name': 'string'
})

products_data = [
    [1, 'keyboard', 120],
    [2, 'mouse', 80],
    [3, 'screen', 600],
    [4, 'hard disk', 450]
]

orders_data = [
    [1,  '2020-07-31', 1, 1],
    [2,  '2020-07-30', 2, 2],
    [3,  '2020-08-29', 3, 3],
    [4,  '2020-07-29', 4, 1],
    [5,  '2020-06-10', 1, 2],
    [6,  '2020-08-01', 2, 1],
    [7,  '2020-08-01', 3, 3],
    [8,  '2020-08-03', 1, 2],
    [9,  '2020-08-07', 2, 3],
    [10, '2020-07-15', 1, 2]
]

orders = pd.DataFrame(
    orders_data,
    columns=['order_id', 'order_date', 'customer_id', 'product_id']
).astype({
    'order_id': 'int64',
    'order_date': 'datetime64[ns]',
    'customer_id': 'int64',
    'product_id': 'int64'
})


products = pd.DataFrame(
    products_data,
    columns=['product_id', 'product_name', 'price']
).astype({
    'product_id': 'int64',
    'product_name': 'string',
    'price': 'int64'
})

print(duckdb.query("""
with cte as
(select customer_id, product_id,count(*) as num_ordered from orders group by customer_id,product_id),

cte2 as
(select *,first_value(num_ordered) Over(partition by customer_id order by num_ordered desc) as most_freq from cte
)

select c.customer_id,p.product_id,p.product_name,
from cte2 c left join products p on c.product_id=p.product_id
where c.num_ordered=c.most_freq

""").to_df())