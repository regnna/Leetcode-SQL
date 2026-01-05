import pandas as pd
import duckdb

data=[
[1,1,0],
[2,1,0],
[11,2,0],
[12,2,1],
[21,3,1],
[22,3,0],
[31,4,1],
[32,4,1]
]

orders=pd.DataFrame(
    data,
    columns=['order_id','customer_id','order_type']
).astype({
    'order_id':'int64',
    'customer_id':'int64',
    'order_type':'int64'
})

print(duckdb.query(""" 
with cte as(
select *,min(order_type) over(partition by customer_id) as minimum from orders
)

select order_id,customer_id,order_type from cte where not (order_type=1 and minimum=0)
""").to_df())