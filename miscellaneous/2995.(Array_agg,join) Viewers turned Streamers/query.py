import pandas as pd
import duckdb 

sessions_data = [
    [101, "2023-11-06 13:53:42", "2023-11-06 14:05:42", 375, "Viewer"],
    [101, "2023-11-22 16:45:21", "2023-11-22 20:39:21", 594, "Streamer"],
    [102, "2023-11-16 13:23:09", "2023-11-16 16:10:09", 777, "Streamer"],
    [102, "2023-11-17 13:23:09", "2023-11-17 16:10:09", 778, "Streamer"],
    [101, "2023-11-20 07:16:06", "2023-11-20 08:33:06", 315, "Streamer"],
    [104, "2023-11-27 03:10:49", "2023-11-27 03:30:49", 797, "Viewer"],
    [103, "2023-11-27 03:10:49", "2023-11-27 03:30:49", 798, "Streamer"],
]

sessions = pd.DataFrame(
    sessions_data,
    columns=[
        "user_id",
        "session_start",
        "session_end",
        "session_id",
        "session_type"
    ]
).astype({
    "user_id": "int64",
    "session_start": "datetime64[ns]",
    "session_end": "datetime64[ns]",
    "session_id": "int64",
    "session_type": "string"
})


print(duckdb.query("""

/*with cte as (
select *,rank() over(partition by user_id order by session_start) as rnk from sessions),
cte2 as(
select * from (select user_id from cte where rnk=1 and session_type='Viewer') left join cte using(user_id))
select user_id, count(*) as sessions_count from (
select * from cte2 where rnk>=2 and session_type='Streamer') group by user_id */

WITH first_behavior AS (
    SELECT 
        user_id,
        (ARRAY_AGG(session_type ORDER BY session_start))[1] AS first_session
    FROM sessions
    GROUP BY user_id
),
streamer_counts AS (
    SELECT 
        user_id,
        COUNT(CASE WHEN session_type = 'Streamer' THEN 1 END) AS cnt
    FROM sessions
    GROUP BY user_id
)
SELECT 
    s.user_id,
    s.cnt AS sessions_count
FROM streamer_counts s
JOIN first_behavior f ON s.user_id = f.user_id
WHERE f.first_session = 'Viewer'
  AND s.cnt > 0
ORDER BY s.cnt DESC, s.user_id DESC;
""").to_df())