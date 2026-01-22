import pandas as pd
import duckdb


data=[
    [1,5],
    [2,6],
    [3,5],
    [3,6],
    [1,6],
    [2,6]
]

customer=pd.DataFrame(data,columns=['customer_id','product_key']).astype({
    "customer_id":'int64',
    "product_key":'int64'
})

data=[
    [5],
    [6]
]

product=pd.DataFrame(data,columns=['product_key']).astype({'product_key':'int64'})

print(duckdb.query("""
with cte as(
select *,count(distinct product_key) over(partition by customer_id) as they_bought from customer 
)

SELECT customer_id
FROM customer
GROUP BY customer_id
HAVING COUNT(DISTINCT product_key) = (SELECT COUNT(*) FROM product);

/*select distinct customer_id
from cte c right join product p using(product_key) where they_bought=(select count(*) from product)*/
""").to_df())