import pandas as pd
import duckdb

data = [[1, 101, '2024-01-05', 150.0, 'purchase'], [2, 101, '2024-01-15', 200.0, 'purchase'], [3, 101, '2024-02-10', 180.0, 'purchase'], [4, 101, '2024-02-20', 250.0, 'purchase'], [5, 102, '2024-01-10', 100.0, 'purchase'], [6, 102, '2024-01-12', 120.0, 'purchase'], [7, 102, '2024-01-15', 80.0, 'refund'], [8, 102, '2024-01-18', 90.0, 'refund'], [9, 102, '2024-02-15', 130.0, 'purchase'], [10, 103, '2024-01-01', 500.0, 'purchase'], [11, 103, '2024-01-02', 450.0, 'purchase'], [12, 103, '2024-01-03', 400.0, 'purchase'], [13, 104, '2024-01-01', 200.0, 'purchase'], [14, 104, '2024-02-01', 250.0, 'purchase'], [15, 104, '2024-02-15', 300.0, 'purchase'], [16, 104, '2024-03-01', 350.0, 'purchase'], [17, 104, '2024-03-10', 280.0, 'purchase'], [18, 104, '2024-03-15', 100.0, 'refund']]
customer_transactions = pd.DataFrame(data,
columns=["transaction_id","customer_id","transaction_date","amount","transaction_type"]).astype({
"transaction_id": "Int64",
    "customer_id": "Int64",
    "transaction_date": "datetime64[ns]",
    "amount": "float64",
    "transaction_type": "object"
})

print(duckdb.query("""

with cte as(
select customer_id,sum(case when transaction_type='purchase' then 1 else 0 end) as num_p,
sum(case when transaction_type='refund' then 1 else 0 end)/count(distinct transaction_id) as refund_rate,
datediff('day',min(transaction_date),max(transaction_date)) as active_days
from customer_transactions 
group by customer_id
order by customer_id
)

select customer_id
from cte
where num_p>=3
and refund_rate<0.20 and
active_days>=30
""").to_df())