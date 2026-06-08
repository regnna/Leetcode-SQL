import pandas as pd
import duckdb

orders_data = [
    [1, 1, "2020-06-01", "1", 10],
    [2, 1, "2020-06-08", "2", 10],
    [3, 2, "2020-06-02", "1", 5],
    [4, 3, "2020-06-03", "3", 5],
    [5, 4, "2020-06-04", "4", 1],
    [6, 4, "2020-06-05", "5", 5],
    [7, 5, "2020-06-05", "1", 10],
    [8, 5, "2020-06-14", "4", 5],
    [9, 5, "2020-06-21", "3", 5],
]

orders = pd.DataFrame(
    orders_data,
    columns=["order_id", "customer_id", "order_date", "item_id", "quantity"]
).astype({
    "order_id": "int64",
    "customer_id": "int64",
    "order_date": "datetime64[ns]",
    "item_id": "string",
    "quantity": "int64"
})

items_data = [
    ["1", "LC Alg. Book",   "Book"],
    ["2", "LC DB. Book",    "Book"],
    ["3", "LC SmarthPhone", "Phone"],
    ["4", "LC Phone 2020",  "Phone"],
    ["5", "LC SmartGlass",  "Glasses"],
    ["6", "LC T-Shirt XL",  "T-Shirt"],
]

items = pd.DataFrame(
    items_data,
    columns=["item_id", "item_name", "item_category"]
).astype({
    "item_id": "string",
    "item_name": "string",
    "item_category": "string"
})


print(duckdb.query("""
with cte as(
select o.*,dayofweek(order_date) as day,item_category as category from orders o right join items i using(item_id) )

,cte2 as(select distinct day, category, sum(quantity) over(partition by  category,day) as val from cte)

, cte3 as(
select distinct category,
case when day=1 then coalesce(val,0) end as Monday,
case when day=2 then coalesce(val,0) end as Tuesday,
case when day=3 then coalesce(val,0) end as Wednesday,
case when day=4 then coalesce(val,0) end as Thursday,
case when day=5 then coalesce(val,0) end as Friday,
case when day=6 then coalesce(val,0) end as Saturday,
case when day=0 then coalesce(val,0) end as Sunday
 from cte2 )

/*
Select category, ifnull(sum(Monday),0) as Monday,
ifnull(sum(Tuesday),0) as Tuesday,
ifnull(sum(Wednesday),0) as Wednesday,
ifnull(sum(Thursday),0) as Thursday,
ifnull(sum(Friday),0) as Friday,
ifnull(sum(Saturday),0) as Saturday,
ifnull(sum(Sunday),0) as Sunday
from cte3 group by category
 order by category*/

/*select * from cte2*/

SELECT 
    i.item_category AS category,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 1 THEN o.quantity END), 0) AS Monday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 2 THEN o.quantity END), 0) AS Tuesday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 3 THEN o.quantity END), 0) AS Wednesday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 4 THEN o.quantity END), 0) AS Thursday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 5 THEN o.quantity END), 0) AS Friday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 6 THEN o.quantity END), 0) AS Saturday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM o.order_date) = 0 THEN o.quantity END), 0) AS Sunday
FROM items i
LEFT JOIN orders o ON i.item_id = o.item_id
GROUP BY i.item_category
ORDER BY i.item_category;
""").to_df())
