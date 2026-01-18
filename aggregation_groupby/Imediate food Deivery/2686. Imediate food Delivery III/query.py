import pandas as pd
import duckdb

# Delivery table data
data = [
    [1, 1, '2019-08-01', '2019-08-02'],
    [2, 2, '2019-08-01', '2019-08-01'],
    [3, 1, '2019-08-01', '2019-08-01'],
    [4, 3, '2019-08-02', '2019-08-13'],
    [5, 3, '2019-08-02', '2019-08-02'],
    [6, 2, '2019-08-02', '2019-08-02'],
    [7, 4, '2019-08-03', '2019-08-03'],
    [8, 1, '2019-08-03', '2019-08-03'],
    [9, 5, '2019-08-04', '2019-08-08'],
    [10, 2, '2019-08-04', '2019-08-18']
]

delivery = pd.DataFrame(
    data,
    columns=[
        'delivery_id',
        'customer_id',
        'order_date',
        'customer_pref_delivery_date'
    ]
).astype({
    'delivery_id': 'int64',
    'customer_id': 'int64',
    'order_date': 'datetime64[ns]',
    'customer_pref_delivery_date': 'datetime64[ns]'
})

print(duckdb.query("""
select order_date,round(sum(if(order_date=customer_pref_delivery_date,1,0))/count(*)*100,2) as immediate_percentage
from  delivery group by order_date,order by order_date
""").to_df())