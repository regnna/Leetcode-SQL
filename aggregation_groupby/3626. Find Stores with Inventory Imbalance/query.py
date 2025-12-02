import pandas as pd
import duckdb

pd.set_option("display.max_columns",200)

data = [[1, 'Downtown Tech', 'New York'], [2, 'Suburb Mall', 'Chicago'], [3, 'City Center', 'Los Angeles'], [4, 'Corner Shop', 'Miami'], [5, 'Plaza Store', 'Seattle']]
stores = pd.DataFrame(data, columns=['store_id', 'store_name', 'location']).astype({'store_id': 'int64', 'store_name': 'string', 'location': 'string'})

data = [[1, 1, 'Laptop', 5, 999.99], [2, 1, 'Mouse', 50, 19.99], [3, 1, 'Keyboard', 25, 79.99], [4, 1, 'Monitor', 15, 299.99], [5, 2, 'Phone', 3, 699.99], [6, 2, 'Charger', 100, 25.99], [7, 2, 'Case', 75, 15.99], [8, 2, 'Headphones', 20, 149.99], [9, 3, 'Tablet', 2, 499.99], [10, 3, 'Stylus', 80, 29.99], [11, 3, 'Cover', 60, 39.99], [12, 4, 'Watch', 10, 299.99], [13, 4, 'Band', 25, 49.99], [14, 5, 'Camera', 8, 599.99], [15, 5, 'Lens', 12, 199.99]]
inventory = pd.DataFrame(data, columns=['inventory_id', 'store_id', 'product_name', 'quantity', 'price']).astype({'inventory_id': 'int64', 'store_id': 'int64', 'product_name': 'string', 'quantity': 'int64', 'price': 'float64'})

print(duckdb.query("""
WITH extremes AS (
  SELECT i.store_id,Any_VALUE(s.store_name) as store_name ,MAX(price) AS store_max_price, MIN(price) AS store_min_price
  FROM inventory i join stores s on i.store_id=s.store_id
  GROUP BY i.store_id,s.store_name
),
max_products AS (
  SELECT i.store_id, STRING_AGG(i.product_name, ', ') AS max_product_names
  FROM inventory i
  JOIN extremes e ON i.store_id = e.store_id AND i.price = e.store_max_price
  GROUP BY i.store_id
),
min_products AS (
  SELECT i.store_id, STRING_AGG(i.product_name, ', ') AS min_product_names
  FROM inventory i
  JOIN extremes e ON i.store_id = e.store_id AND i.price = e.store_min_price
  GROUP BY i.store_id
),

/*select * from max_products;
select * from stores;*/
/*
SELECT
  e.store_id,
  any_Value(e.store_name) as store_name,
  e.store_max_price,
  mp.max_product_names,
  SUM(CASE WHEN i.price = e.store_max_price THEN i.quantity ELSE 0 END) AS qty_at_max_price,
  e.store_min_price,
  mn.min_product_names,
  SUM(CASE WHEN i.price = e.store_min_price THEN i.quantity ELSE 0 END) AS qty_at_min_price
FROM extremes e
JOIN inventory i ON i.store_id = e.store_id
Join stores s on s.store_id=e.store_id
LEFT JOIN max_products mp ON mp.store_id = e.store_id
LEFT JOIN min_products mn ON mn.store_id = e.store_id

GROUP BY e.store_id, e.store_max_price, mp.max_product_names, e.store_min_price, mn.min_product_names
ORDER BY e.store_id*/


cte as
(SELECT *,row_number() over(PARTITION by store_id order by price DESC) as exp_rnk, row_number() over(partition by store_id order by price) as chp_rnk
from inventory),

cte2 as(
select store_id,
MAX(case when exp_rnk=1 then product_name else Null end) MOST_EXP_Product,
MAX(case when chp_rnk=1 then product_name else Null end) MOST_CHP_Product,
sum(CASE WHEN exp_rnk=1 then quantity else 0 end) as exp_Qty,
sum(case when chp_rnk=1 then quantity else 0 end) as chp_Qty,
round(sum(case when chp_rnk=1 then quantity else 0 end)/sum(CASE WHEN exp_rnk=1 then quantity else 0 end),2)  imballance_ratio

from cte group by store_id
having count(distinct product_name)>=3
)

select s.*,c.MOST_EXP_Product,c.MOST_CHP_Product,c.imballance_ratio
from cte2 c left join stores s on c.store_id=s.store_id
where c.exp_Qty<c.chp_Qty
order by c.imballance_ratio desc, s.store_name
;


""").to_df())