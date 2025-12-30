import pandas as pd
import duckdb

data = [
    ['2020-05-01', 'apples', 10],
    ['2020-05-01', 'oranges', 8],
    ['2020-05-02', 'apples', 15],
    ['2020-05-02', 'oranges', 15],
    ['2020-05-03', 'apples', 20],
    ['2020-05-03', 'oranges', 0],
    ['2020-05-04', 'apples', 15],
    ['2020-05-04', 'oranges', 16],
]

sales = pd.DataFrame(
    data,
    columns=['sale_date', 'fruit', 'sold_num']
).astype({
    'sale_date': 'datetime64[ns]',
    'fruit': 'string',
    'sold_num': 'int64'
})

print(duckdb.query("""


select sale_date,sum(if(fruit='apples',sold_num,-1*sold_num )) as diff
 from sales group by sale_date order by sale_date 
""").to_df()) 
