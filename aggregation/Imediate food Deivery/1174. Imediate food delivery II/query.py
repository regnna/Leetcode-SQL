import pandas as pd
import duckdb

data = [
    [1, 1, '2019-08-01', '2019-08-02'],
    [2, 2, '2019-08-02', '2019-08-02'],
    [3, 1, '2019-08-11', '2019-08-12'],
    [4, 3, '2019-08-24', '2019-08-24'],
    [5, 3, '2019-08-21', '2019-08-22'],
    [6, 2, '2019-08-11', '2019-08-13'],
    [7, 4, '2019-08-09', '2019-08-09']
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
with cte as(
select distinct customer_id,first_value(order_date) over(partition by customer_id order by order_date ) as firstod,
first_value(customer_pref_delivery_date) over(partition by customer_id) as firstdd from delivery
)

select sum(if(firstod=firstdd,1,0))/count(*)*100 as immediate_percentage from cte 

/*select customer_id,if(customer_pref_delivery_date>order_date,0,1) as imediate_order from delivery 
select delivery_id, if(customer_pref_delivery_date>order_date,0,1) as imediate_order from delivery*/
""").to_df())