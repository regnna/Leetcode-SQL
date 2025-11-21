import duckdb
import pandas as pd

data = [[1, 'Electronics', 1000], [2, 'Clothing', 50], [3, 'Electronics', 1200], [4, 'Home', 500]]
products = pd.DataFrame(data,columns=['product_id', 'category', 'price']).astype({'product_id':'Int64', 'category':'object', 'price':'Int64'})

data = [['Electronics', 10], ['Clothing', 20]]
discounts = pd.DataFrame(data,columns=['category','discount']).astype({'category': "object", 'discount': 'Int64'})

print(duckdb.query("""
select p.product_id, 
p.price * (100-ifnull(d.discount,0) )/100 as final_price, p.category             -- easy arithmetic joinning 
from products p left join discounts d 
-- Using (category)
on p.category = d.category
order by 1;
-- order by product_id asc;
-- order by p.product_id ;



""").to_df())