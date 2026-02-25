import pandas as pd
import duckdb


insurance_data = [
    [1, 10.0, 5.0, 10.0, 10.0],
    [2, 20.0, 20.0, 20.0, 20.0],
    [3, 10.0, 30.0, 20.0, 20.0],
    [4, 10.0, 40.0, 40.0, 40.0],
    [5, 10.0,5,20,20]
]

insurance = pd.DataFrame(
    insurance_data,
    columns=['pid', 'tiv_2015', 'tiv_2016', 'lat', 'lon']
).astype({
    'pid': 'int64',
    'tiv_2015': 'float64',
    'tiv_2016': 'float64',
    'lat': 'float64',
    'lon': 'float64'
})

print(duckdb.query("""
/*select sum(tiv_2016) as tiv_2016 from(
select pid,tiv_2016,count(loc) over(partition by loc) as cnt_loc,count(tiv_2015) over(partition by tiv_2015) cnt_ins_2015 from(
select *,concat(lat,lon) as loc from insurance)
/*select sum(tiv_2016) from insurance where tiv_2015 group by tiv_2015 whe*/
) where cnt_loc=1*/


SELECT ROUND(SUM(tiv_2016), 2) AS tiv_2016
FROM (
    SELECT 
        pid,
        tiv_2016,
        -- Condition 1: Location must be unique (count = 1)
        COUNT(*) OVER (PARTITION BY lat, lon) AS loc_count,
        -- Condition 2: tiv_2015 must appear at least twice
        COUNT(*) OVER (PARTITION BY tiv_2015) AS tiv_2015_count
    FROM insurance
) 
WHERE loc_count = 1 
  AND tiv_2015_count >= 2;
""").to_df())
