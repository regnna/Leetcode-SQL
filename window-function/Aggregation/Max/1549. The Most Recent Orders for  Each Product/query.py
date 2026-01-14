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
    [1, '2020-07-31', 1, 1],
    [2, '2020-07-30', 2, 2],
    [3, '2020-08-29', 3, 3],
    [4, '2020-07-29', 4, 1],
    [5, '2020-06-10', 1, 2],
    [6, '2020-08-01', 2, 1],
    [7, '2020-08-01', 3, 1],
    [8, '2020-08-03', 1, 2],
    [9, '2020-08-07', 2, 3],
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

products_data = [
    [1, 'keyboard', 120],
    [2, 'mouse', 80],
    [3, 'screen', 600],
    [4, 'hard disk', 450]
]

products = pd.DataFrame(
    products_data,
    columns=['product_id', 'product_name', 'price']
).astype({
    'product_id': 'int64',
    'product_name': 'string',
    'price': 'int64'
})

print(duckdb.query("""
/*
select product_id,order_id,
max(order_date) over(partition by product_name order by product_name,product_id,order_id) last_buy

from orders left join products using(product_id)
*/
with cte as(
select distinct product_id,Product_name,max(order_date) as od from orders left join products using(product_id) group by product_name,product_id order by product_name,product_id
)
select product_name, o.product_id, order_id, order_date from orders o left join cte c on (c.od=o.order_date and c.product_id=o.product_id) where product_name not null order by product_name,o.product_id,order_id
""").to_df())

