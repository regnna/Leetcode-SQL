import pandas as pd
import duckdb

sales=pd.DataFrame([
    [1,1,101,10],
    [2,3,101,7],
    [3,1,102,9],
    [4,2,102,6],
    [5,3,102,10],
    [6,1,102,6]
],columns=['sale_id','product_id','user_id','quantity']).astype({
    'sale_id':'int64',
    'product_id':'int64',
    'user_id':'int64',
    'quantity':'int64'
})

product=pd.DataFrame([
    [1,10],
    [2,25],
    [3,15]
],columns=['product_id','price']).astype({
    'product_id':'int64',
    'price':'int64'
})

print(duckdb.query("""
/*
select distinct user_id,product_id from(
select user_id, product_id,Product_Quant,price,max(Product_Quant*price) over(partition by user_id) as max_cost from(
select s.*,sum(s.quantity) over(partition by user_id,s.product_id) Product_Quant,
p.price from sales s left join product p on s.product_id=p.product_id
)) where Product_Quant*price= max_cost
*/

explain analyze SELECT user_id, product_id
FROM (
    SELECT 
        s.user_id,
        s.product_id,
        SUM(s.quantity * p.price) AS total_spend,
        MAX(SUM(s.quantity * p.price)) OVER (PARTITION BY s.user_id) AS max_spend
    FROM sales s
    JOIN product p ON s.product_id = p.product_id
    GROUP BY s.user_id, s.product_id
) t
WHERE total_spend = max_spend;


""").to_df())