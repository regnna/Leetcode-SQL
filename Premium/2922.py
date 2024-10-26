import pandas as pd
from datetime import datetime
import duckdb

users_data = {
    "seller_id": [1, 2, 3],
    "join_date": [datetime(2019, 1, 1), datetime(2019, 2, 9), datetime(2019, 1, 19)],
    "favorite_brand": ["Lenovo", "Samsung", "LG"]
}
users_df = pd.DataFrame(users_data)


orders_data = {
    "order_id": [1, 2, 3, 4, 5],
    "order_date": [datetime(2019, 8, 1), datetime(2019, 8, 2), datetime(2019, 8, 3), datetime(2019, 8, 4), datetime(2019, 8, 4)],
    "item_id": [4, 2, 3, 1, 4],
    "seller_id": [2, 3, 3, 2, 2]
}
orders_df = pd.DataFrame(orders_data)

# Define data for Items table
items_data = {
    "item_id": [1, 2, 3, 4],
    "item_brand": ["Samsung", "Lenovo", "LG", "HP"]
}
items_df = pd.DataFrame(items_data)


print(duckdb.query("""

with cte as 
(
select od.seller_id, count(Distinct od.item_id) as num_items
from items_df id left join orders_df od  using (item_id) left join users_df ud using (seller_id)
where id.item_brand<>ud.favorite_brand
group by od.seller_id
),


cte2 as
(
select *,Dense_Rank() Over(order by num_items Desc) as rnk from cte
)

select seller_id, num_items
from cte2
where rnk=1
order by seller_id;




""").to_df())