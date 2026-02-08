import pandas as pd
import duckdb

transactions_data = [
    [1, '2021-11-07', 'Deposit',  2000],
    [1, '2021-11-09', 'Withdraw', 1000],
    [1, '2021-11-11', 'Deposit',  3000],
    [2, '2021-12-07', 'Deposit',  7000],
    [2, '2021-12-12', 'Withdraw', 7000]
]

transactions = pd.DataFrame(
    transactions_data,
    columns=['account_id', 'day', 'type', 'amount']
).astype({
    'account_id': 'int64',
    'day': 'datetime64[ns]',
    'type': 'string',        # ENUM-like
    'amount': 'int64'
})


print(duckdb.query("""
select account_id,day,
SUM(
    CASE 
        WHEN type = 'Deposit' THEN amount 
        ELSE -amount 
    END
) OVER (
    PARTITION BY account_id
    ORDER BY day
) AS balance

 from transactions

""").to_df())