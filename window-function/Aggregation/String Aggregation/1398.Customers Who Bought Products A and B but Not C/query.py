import pandas as pd
import duckdb

data=[
    [1,'Daniel'],
    [2,'Diana'],
    [3,'Elizabeth'],
    [4,'Jhon']    
]

customers=pd.DataFrame(data,
columns=['customer_id','customer_name']
).astype({
    'customer_id':'int64',
    'customer_name':'str'
})

# Orders table
orders_data = [
    [10, 1, 'A'],
    [20, 1, 'B'],
    [30, 1, 'C'],
    [40, 1, 'A'],
    [50, 2, 'A'],
    [60, 3, 'A'],
    [70, 3, 'B'],
    [80, 3, 'B'],
    [90, 4, 'C']
]

orders = pd.DataFrame(
    orders_data,
    columns=['order_id', 'customer_id', 'product_name']
).astype({
    'order_id': 'int64',
    'customer_id': 'int64',
    'product_name': 'string'
})

print(duckdb.query("""
with cte as
(select customer_id
from orders 
group by customer_id
having sum(product_name='A')>0 and sum(product_name='B')>0 and sum(product_name='C')=0
)

select c.customer_id,cu.customer_name
from cte c left join customers cu on c.customer_id=cu.customer_id



/*
/*working but lengthy */
with cte as(
select distinct
customer_id,
string_agg(product_name,'') within group ( order by product_name) over(partition by customer_id) as purchased_products
from orders
),
cte2 as(
select customer_id,purchased_products
from cte
where purchased_products like '%AB%'  and purchased_products not like '%C%' order by customer_id
)

select c.customer_id,cu.customer_name
from cte2 c left join customers cu using(customer_id)
*/
""").to_df())