import pandas as pd
import duckdb

# Transactions data
data = [
    [121, 'US', 'approved', 1000, '2018-12-18'],
    [122, 'US', 'declined', 2000, '2018-12-19'],
    [123, 'US', 'approved', 2000, '2019-01-01'],
    [124, 'DE', 'approved', 2000, '2019-01-07']
]

transactions = pd.DataFrame(
    data,
    columns=['id', 'country', 'state', 'amount', 'trans_date']
).astype({
    'id': 'int64',
    'country': 'string',
    'state': 'string',          # ENUM-like
    'amount': 'int64',
    'trans_date': 'datetime64[ns]'
})

# Register with DuckDB
# duckdb.register("transactions", transactions)


print(duckdb.query("""
select strftime(trans_date,'%Y-%m') as month, country, count(*) as trans_count, sum(if(state='approved',1,0)) as approved_count, sum(amount) as trans_total_amount, sum(if(state='approved',amount,0)) approved_total_amount
from transactions group by strftime(trans_date,'%Y-%m'),country
""").to_df())
