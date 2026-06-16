import pandas as pd
import duckdb

product_purchases = pd.DataFrame(
    [
        [1, 101, 2],
        [1, 102, 1],
        [1, 201, 3],
        [1, 301, 1],
        [2, 101, 1],
        [2, 102, 2],
        [2, 103, 1],
        [2, 201, 5],
        [3, 101, 2],
        [3, 103, 1],
        [3, 301, 4],
        [3, 401, 2],
        [4, 101, 1],
        [4, 201, 3],
        [4, 301, 1],
        [4, 401, 2],
        [5, 102, 2],
        [5, 103, 1],
        [5, 201, 2],
        [5, 202, 3],
    ],
    columns=["user_id", "product_id", "quantity"]
).astype({
    "user_id": "int64",
    "product_id": "int64",
    "quantity": "int64"
})

product_info = pd.DataFrame(
    [
        [101, "Electronics", 100.0],
        [102, "Books", 20.0],
        [103, "Books", 35.0],
        [201, "Clothing", 45.0],
        [202, "Clothing", 60.0],
        [301, "Sports", 75.0],
        [401, "Kitchen", 50.0],
    ],
    columns=["product_id", "category", "price"]
).astype({
    "product_id": "int64",
    "category": "string",
    "price": "float64"
})

print(duckdb.query("""
with cte as(
select p1.*,p2.category from product_purchases p1 left join product_info p2 using(product_id)
)

select c1.category as cat1,c2.category as cat2,count(distinct c1.user_id) as customer_count
from cte c1 left join cte as c2
on c1.user_id=c2.user_id and c1.category<c2.category
where c2.category <> 'NULL'

group by c1.category,c2.category
having count(distinct c1.user_id)>=3
order by customer_count desc, cat1,cat2
""").to_df())