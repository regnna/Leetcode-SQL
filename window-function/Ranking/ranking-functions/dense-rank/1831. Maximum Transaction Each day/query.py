import pandas as pd
import duckdb

'''
| Step          | DENSE_RANK | FIRST_VALUE       |
| ------------- | ---------- | ----------------- |
| Identify max  | Rank = 1   | First row value   |
| Mark winners  | Built-in   | Manual comparison |
| Handles ties  | Naturally  | Indirectly        |
| Easy to read  | Yes        | Medium            |
| Easy to break | Hard       | Easy              |

'''

data = [
    [8, '2021-04-03 15:57:28', 57],
    [9, '2021-04-28 08:47:25', 10],
    [1, '2021-04-29 13:28:30', 58],
    [5, '2021-04-28 16:39:59', 40],
    [6, '2021-04-29 23:39:28', 58]
]
transactions=pd.DataFrame(data,
    columns=['transaction_id','day','amount']
).astype({
    'transaction_id':'int64',
    'day':'datetime64[ns]',
    'amount':'int64'
})


print(duckdb.query("""
/*
with cte as(
select transaction_id,amount,first_value(amount) over(partition by DATE(day) order by amount desc) max_dat_day from transactions 
)
select transaction_id from cte where amount=max_dat_day order by transaction_id
*/

SELECT transaction_id
FROM (
    SELECT
        transaction_id,
        DENSE_RANK() OVER (
            PARTITION BY DATE(day)
            ORDER BY amount DESC
        ) AS rnk
    FROM transactions
) t
WHERE rnk = 1
ORDER BY transaction_id;

""").to_df())