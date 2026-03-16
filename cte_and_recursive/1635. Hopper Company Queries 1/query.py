import pandas as pd
import duckdb

data = [[10, '2019-12-10'], [8, '2020-1-13'], [5, '2020-2-16'], [7, '2020-3-8'], [4, '2020-5-17'], [1, '2020-10-24'], [6, '2021-1-5']]
drivers = pd.DataFrame(data, columns=['driver_id', 'join_date']).astype({'driver_id':'Int64', 'join_date':'datetime64[ns]'})
data = [[6, 75, '2019-12-9'], [1, 54, '2020-2-9'], [10, 63, '2020-3-4'], [19, 39, '2020-4-6'], [3, 41, '2020-6-3'], [13, 52, '2020-6-22'], [7, 69, '2020-7-16'], [17, 70, '2020-8-25'], [20, 81, '2020-11-2'], [5, 57, '2020-11-9'], [2, 42, '2020-12-9'], [11, 68, '2021-1-11'], [15, 32, '2021-1-17'], [12, 11, '2021-1-19'], [14, 18, '2021-1-27']]
rides = pd.DataFrame(data, columns=['ride_id', 'user_id', 'requested_at']).astype({'ride_id':'Int64', 'user_id':'Int64', 'requested_at':'datetime64[ns]'})
data = [[10, 10, 63, 38], [13, 10, 73, 96], [7, 8, 100, 28], [17, 7, 119, 68], [20, 1, 121, 92], [5, 7, 42, 101], [2, 4, 6, 38], [11, 8, 37, 43], [15, 8, 108, 82], [12, 8, 38, 34], [14, 1, 90, 74]]
accepted_rides = pd.DataFrame(data, columns=['ride_id', 'driver_id', 'ride_distance', 'ride_duration']).astype({'ride_id':'Int64', 'driver_id':'Int64', 'ride_distance':'Int64', 'ride_duration':'Int64'})

print(duckdb.query("""
with Recursive cte as
(
    select 2020 as year, 1 as month
    Union select year, month+1 from cte
    where month <12
),
cte2 as
(
select  c.month, count(d.driver_id) as active_drivers from cte as c 
left join drivers as d
on last_day(cast(concat(c.year, '-', c.month, '-01') as date))
>= d.join_date
group by c.month
order by c.month
),

cte3 as
(
select month(r.requested_at) as month,
count(r.ride_id) as accepted_rides
from rides as r inner join accepted_rides as a
using (ride_id) 
where Year(r.requested_at)=2020
group by MONTH(r.requested_at)
)

select cte2.*,ifNull(cte3.accepted_rides,0) as accepted_rides
from cte2
left join cte3
using(month)
order by cte2.month;
""").to_df())
