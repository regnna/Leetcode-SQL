import pandas as pd
import duckdb 

data = [[1, 'Warm Jacket', 'Apparel'], [2, 'Designer Jeans', 'Apparel'], [3, 'Cutting Board', 'Kitchen'], [4, 'Smart Speaker', 'Tech'], [5, 'Yoga Mat', 'Fitness']]
products = pd.DataFrame(
    data,
    columns=['product_id', 'product_name', 'category']).astype({'product_id': 'int64', 'product_name': 'string', 'category': 'string'})

data = [[1, 1, '2023-01-15', 5, 10.0], [2, 2, '2023-01-20', 4, 15.0], [3, 3, '2023-03-10', 3, 18.0], [4, 4, '2023-04-05', 1, 20.0], [5, 1, '2023-05-20', 2, 10.0], [6, 2, '2023-06-12', 4, 15.0], [7, 5, '2023-06-15', 5, 12.0], [8, 3, '2023-07-24', 2, 18.0], [9, 4, '2023-08-01', 5, 20.0], [10, 5, '2023-09-03', 3, 12.0], [11, 1, '2023-09-25', 6, 10.0], [12, 2, '2023-11-10', 4, 15.0], [13, 3, '2023-12-05', 6, 18.0], [14, 4, '2023-12-22', 3, 20.0], [15, 5, '2024-02-14', 2, 12.0]]
sales = pd.DataFrame(
    data,columns=['sale_id', 'product_id', 'sale_date', 'quantity', 'price']).astype({'sale_id': 'int64', 'product_id': 'int64', 'sale_date': 'datetime64[ns]', 'quantity': 'int64', 'price': 'float64'})


print(duckdb.query("""
with cte as
(
select s.*,p.category,
case when Month(Date(sale_date)) in (12,1,2) then 'Winter'
when Month(Date(sale_date)) in(3,4,5) then 'Spring'
when month(DATE(sale_date)) in (6,7,8) then 'Summer'
else 'Fall' end as season

from sales s left join products p on s.product_id=p.product_id
),

cte2 as(
select season, category,(sum(quantity)) total_quantity, sum(quantity*price) total_revenue
from cte group by season,category
),
cte3 as(
select *,
 row_number() over(partition by season order by total_quantity desc,total_revenue desc) as rnk
from cte2
)

select season,category,total_quantity,total_revenue from cte3 where rnk=1
order by season asc;
""").to_df())