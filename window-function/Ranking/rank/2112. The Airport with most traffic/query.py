import pandas as pd
import duckdb


data=[
    [1,2,4],
    [2,1,5],
    [3,4,5],
    [4,3,4],
    [5,6,7]
]

flights=pd.DataFrame(data,
    columns=['departure_airport','arrival_airport','flights_count']).astype({
        'departure_airport':'int64',
        'arrival_airport':'int64',
        'flights_count':'int64'
    })

print(duckdb.query("""
with cte as(
select departure_airport as port,sum(flights_count) over(partition by departure_airport) as flys
 from flights
),
cte2 as(
select arrival_airport as port,sum(flights_count) over(partition by arrival_airport) as flys2
 from flights
),
cte3 as(
select 
        ifnull(c1.port,c2.port) as airport_id,
        
    from cte c1 full outer join cte2 c2 on c1.port= c2.port
)

select airport_id from cte3 where total_fly_rnk=1
""").to_df())