import pandas as pd
import duckdb

data = [
    [1, 1, 12],
    [1, 2, 10],
    [1, 3, 15],
    [2, 1, 8],
    [2, 4, 4],
    [2, 5, 6],
    [3, 3, 5],
    [3, 4, 18],
    [4, 5, 2],
    [4, 6, 8],
    [5, 7, 9],
    [5, 8, 9],
    [3, 9, 20],
    [2, 9, 4]
]

orders_details = pd.DataFrame(
    data,
    columns=["order_id", "product_id", "quantity"]
).astype({
    "order_id": "int64",
    "product_id": "int64",
    "quantity": "int64"
})

print(duckdb.query("""

with cte as(
select order_id,
sum(quantity)/sum(count(*)) over(partition by order_id) as average_quantity,
max(quantity) as maxx
from orders_details group by order_id

),

cte2 as(
select c1.order_id, c1.maxx,c2.average_quantity
from cte c1 cross join cte c2 
)

select order_id from cte2 group by order_id,maxx having maxx>max(average_quantity)


/*select distinct order_id from cte2 where not (maxx >average_quantity)
select order_id from cte
 where average_quantity >(select maxx from cte)*/
""").to_df())