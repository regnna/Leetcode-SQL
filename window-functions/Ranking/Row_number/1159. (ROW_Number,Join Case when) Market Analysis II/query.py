import pandas as pd
import duckdb

users_data = [
    [1, "2019-01-01", "Lenovo"],
    [2, "2019-02-09", "Samsung"],
    [3, "2019-01-19", "LG"],
    [4, "2019-05-21", "HP"],
]

users = pd.DataFrame(
    users_data,
    columns=["user_id", "join_date", "favorite_brand"]
).astype({
    "user_id": "int64",
    "join_date": "datetime64[ns]",
    "favorite_brand": "string"
})

orders_data = [
    [1, "2019-08-01", 4, 1, 2],
    [2, "2019-08-02", 2, 1, 3],
    [3, "2019-08-03", 3, 2, 3],
    [4, "2019-08-04", 1, 4, 2],
    [5, "2019-08-04", 1, 3, 4],
    [6, "2019-08-05", 2, 2, 4],
]

orders = pd.DataFrame(
    orders_data,
    columns=["order_id", "order_date", "item_id", "buyer_id", "seller_id"]
).astype({
    "order_id": "int64",
    "order_date": "datetime64[ns]",
    "item_id": "int64",
    "buyer_id": "int64",
    "seller_id": "int64"
})

items_data = [
    [1, "Samsung"],
    [2, "Lenovo"],
    [3, "LG"],
    [4, "HP"],
]

items = pd.DataFrame(
    items_data,
    columns=["item_id", "item_brand"]
).astype({
    "item_id": "int64",
    "item_brand": "string"
})


print(duckdb.query("""
with cte as(
select u.user_id,seller_id,item_brand,favorite_brand from users u left join
(select seller_id,item_brand from (
select *,row_number() over(partition by seller_id order by order_date) as rnk from orders) as c left join items as i on c.item_id=i.item_id where rnk=2) ct on u.user_id=ct.seller_id)


select user_id as seller_id, case when item_brand=favorite_brand and seller_id is not null then 'yes' else 'no' end as '2nd_item_fav_brand' from cte
""").to_df())