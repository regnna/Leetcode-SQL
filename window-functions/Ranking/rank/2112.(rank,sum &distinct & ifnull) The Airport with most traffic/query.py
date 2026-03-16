import pandas as pd
import duckdb


data=[
    [1,2,4],
    [2,1,5],
    [3,4,5],
    [4,3,4],
    [5,6,7]
]
data2=[
    [1,2,4],
    [2,1,5],
    [2,4,5]
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
select distinct
        ifnull(c1.port,c2.port) as airport_id,
        c1.flys+c2.flys2 as sum_traffic,
        rank() over(order by sum_traffic desc ) as total_fly_rnk
        
    from cte c1 full outer join cte2 c2 on c1.port= c2.port
)

select airport_id from cte3 where total_fly_rnk=1
/*select * from cte3*/
""").to_df())