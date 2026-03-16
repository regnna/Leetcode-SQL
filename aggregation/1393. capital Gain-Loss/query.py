import pandas as pd
import duckdb

data = [
    ['Leetcode', 'Buy', 1, 1000],
    ['Corona Masks', 'Buy', 2, 10],
    ['Leetcode', 'Sell', 5, 9000],
    ['Handbags', 'Buy', 17, 30000],
    ['Corona Masks', 'Sell', 3, 1010],
    ['Corona Masks', 'Buy', 4, 1000],
    ['Corona Masks', 'Sell', 5, 500],
    ['Corona Masks', 'Buy', 6, 1000],
    ['Handbags', 'Sell', 29, 7000],
    ['Corona Masks', 'Sell', 10, 10000]
]



stocks = pd.DataFrame(
    data,
    columns=['stock_name', 'operation', 'operation_day', 'price']
).astype({
    'stock_name': 'string',
    'operation': 'string',          # ENUM-like
    'operation_day': 'int64',
    'price': 'int64'
})

print(duckdb.query("""
/*select distinct stock_name,sum(if(operation='Sell',price,-1*price)) over(partition by stock_name) as capital_gain_loss from stocks */

select stock_name,
sum(
case 
    when operation='Sell' 
        then price
    else -1*price
end
) as capital_gain_loss from stocks group by stock_name
""").to_df())