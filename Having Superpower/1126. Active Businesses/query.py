import pandas as pd
import duckdb

# Events table data
data = [
    [1, 'reviews', 7],
    [3, 'reviews', 3],
    [1, 'ads', 11],
    [2, 'ads', 7],
    [3, 'ads', 6],
    [1, 'page views', 3],
    [2, 'page views', 12]
]

events = pd.DataFrame(
    data,
    columns=['business_id', 'event_type', 'occurrences']
).astype({
    'business_id': 'int64',
    'event_type': 'string',
    'occurrences': 'int64'
})


print(duckdb.query("""
with cte as(
select business_id,event_type,occurrences,avg(occurrences) over(partition by event_type) avg_occ from events order by business_id
),
cte2 as(
select business_id,sum(if(occurrences>avg_occ,1,0)) gg from cte group by business_id
)

select business_id from cte2 where gg>1


/*
WITH EventAverages AS (
    SELECT 
        business_id, 
        occurrences,
        AVG(occurrences) OVER(PARTITION BY event_type) as avg_occ
    FROM Events
)
SELECT business_id
FROM EventAverages
WHERE occurrences > avg_occ
GROUP BY business_id
HAVING COUNT(*) > 1;
*/
""").to_df())