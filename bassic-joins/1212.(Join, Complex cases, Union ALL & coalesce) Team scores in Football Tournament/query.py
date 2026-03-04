import pandas as pd
import duckdb

teams=pd.DataFrame(
    [
        [10,'Leetcode FC'],
        [20,'NewYork FC'],
        [30,'Atlanta FC'],
        [40,'Chicago FC'],
        [50,'Toronto FC']
    ]
,columns=['team_id','team_name']).astype({
    'team_id':'int64',
    'team_name':'string'
})

matches = pd.DataFrame({
    'match_id': [1, 2, 3, 4, 5],
    'host_team': [10, 30, 10, 20, 50],
    'guest_team': [20, 10, 50, 30, 30],
    'host_goals': [3, 2, 5, 1, 1],
    'guest_goals': [0, 2, 1, 0, 0]
})



print(duckdb.query("""

/*WITH team_performance AS (
    -- Host teams
    SELECT 
        host_team AS team_id,
        CASE 
            WHEN host_goals > guest_goals THEN 3
            WHEN host_goals = guest_goals THEN 1
            ELSE 0
        END AS points
    FROM matches
    
    UNION ALL
    
    -- Guest teams  
    SELECT 
        guest_team AS team_id,
        CASE 
            WHEN guest_goals > host_goals THEN 3
            WHEN guest_goals = host_goals THEN 1
            ELSE 0
        END AS points
    FROM matches
)
SELECT 
    t.team_id,
    t.team_name,
    COALESCE(SUM(tp.points), 0) AS num_points
FROM teams t
LEFT JOIN team_performance tp ON t.team_id = tp.team_id
GROUP BY t.team_id, t.team_name
ORDER BY num_points DESC, t.team_id ASC;*/

SELECT 
    t.team_id,
    t.team_name,
    COALESCE(SUM(m.points), 0) AS num_points
FROM teams t
LEFT JOIN LATERAL (
    -- Host matches
    SELECT CASE 
        WHEN host_goals > guest_goals THEN 3
        WHEN host_goals = guest_goals THEN 1
        ELSE 0
    END AS points
    FROM matches WHERE host_team = t.team_id
    
    UNION ALL
    
    -- Guest matches
    SELECT CASE 
        WHEN guest_goals > host_goals THEN 3
        WHEN guest_goals = host_goals THEN 1
        ELSE 0
    END
    FROM matches WHERE guest_team = t.team_id
) m ON true
GROUP BY t.team_id, t.team_name
ORDER BY num_points DESC, t.team_id;


""").to_df())