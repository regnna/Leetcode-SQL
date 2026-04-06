import pandas as pd
import duckdb

customers=pd.DataFrame([
    [1,'Alice'],
    [4,'Bob'],
    [5,'Charlie']
],columns=['customer_id','customer_name']).astype({
    'customer_id':'int64',
    'customer_name':'string'
})

print(duckdb.query("""
with recursive cte as(

select 1 as ids
union
select ids+1
from cte
where ids<(select max(customer_id) from customers)

)

select ids from cte where ids not in (select customer_id from customers) 
""").to_df())