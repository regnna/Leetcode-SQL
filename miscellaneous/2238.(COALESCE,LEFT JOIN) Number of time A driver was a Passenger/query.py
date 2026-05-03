import pandas as pd
import duckdb

rides_data = [
    [1, 7, 1],
    [2, 7, 2],
    [3, 11, 1],
    [4, 11, 7],
    [5, 11, 7],
    [6, 11, 3],
]

rides = pd.DataFrame(
    rides_data,
    columns=["ride_id", "driver_id", "passenger_id"]
).astype({
    "ride_id": "int64",
    "driver_id": "int64",
    "passenger_id": "int64"
})

print(duckdb.query("""
/*

with cte as(
select distinct driver_id from rides
)

select 
distinct c.driver_id,
/*d.passenger_id,*/
count(d.passenger_id) over(partition by c.driver_id) cnt
from cte c left join rides d on c.driver_id=d.passenger_id 
*/


with cte as(
select passenger_id as driver_id,count(*) as num_of_times from rides group by passenger_id)

select distinct driver_id,coalesce(num_of_times,0) as cnt from rides left join cte using(driver_id)

/*
SELECT 
    d.driver_id,
    COALESCE(p.cnt, 0) AS cnt
FROM (
    SELECT DISTINCT driver_id FROM rides
) d
LEFT JOIN (
    SELECT passenger_id, COUNT(*) AS cnt
    FROM rides
    GROUP BY passenger_id
) p ON d.driver_id = p.passenger_id;*/
""").to_df())