import pandas as pd
import duckdb

products_data = [
    [1, 100],
    [2, 200],
]

products = pd.DataFrame(
    products_data,
    columns=["product_id", "price"]
).astype({
    "product_id": "int64",
    "price": "int64"
})


purchases_data = [
    [1, 1, 2],
    [3, 2, 1],
    [2, 2, 3],
    [2, 1, 4],
    [4, 1, 10],
]

purchases = pd.DataFrame(
    purchases_data,
    columns=["invoice_id", "product_id", "quantity"]
).astype({
    "invoice_id": "int64",
    "product_id": "int64",
    "quantity": "int64"
})

print(duckdb.query("""

with cte as(
select p.*,price,sum((price*p.quantity)) over(partition by invoice_id,product_id) as product_price, sum((price*p.quantity)) over(partition by invoice_id) as invoice_price from purchases p left join products pt usinG(product_id) 
),
cte2 as(
select min(invoice_id)  from cte where invoice_price=(select max(invoice_price) from cte)
)

select product_id,quantity,product_price as price from cte where invoice_id=(select min(invoice_id)  from cte where invoice_price=(select max(invoice_price) from cte))


""").to_df())