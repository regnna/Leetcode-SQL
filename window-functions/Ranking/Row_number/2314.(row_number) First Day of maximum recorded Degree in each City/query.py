import pandas as pd
import duckdb

import pandas as pd
import duckdb

weather = pd.DataFrame(
    [
        [1, '2022-01-07', -12],
        [1, '2022-03-07', 5],
        [1, '2022-07-07', 24],
        [2, '2022-08-07', 37],
        [2, '2022-08-17', 37],
        [3, '2022-02-07', -7],
        [3, '2022-12-07', -6],
    ],
    columns=['city_id', 'day', 'degree']
).astype({
    'city_id': 'int64',
    'day': 'datetime64[ns]',
    'degree': 'int64'
})


print(duckdb.query("""
/* with cte as(
select *,row_number() OVER (PARTITION BY city_id ORDER BY day ASC) AS rn  from (
    select *, max(degree) over(partition by city_id) mx 
    from weather
)
where degree=mx
)
select city_id, day,degree from cte where rn=1 order by city_id */

SELECT city_id, day, degree
FROM (b
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY city_id
               ORDER BY degree DESC, day ASC
           ) AS rn
    FROM weather
) t
WHERE rn = 1
ORDER BY city_id;


""").to_df())
