import pandas as pd
import duckdb

teams=pd.DataFrame([
    [1,'Ajax'],
    [4,'Dortmund'],
    [6,'Arsenal']
],columns=['team_id','team_name']).astype({
    'team_id':'int64',
    'team_name':'string'
})

matches_data = [
    [1, 4, 0, 1],
    [1, 6, 3, 3],
    [4, 1, 5, 2],
    [6, 1, 0, 0]
]

matches = pd.DataFrame(
    matches_data,
    columns=[
        'home_team_id',
        'away_team_id',
        'home_team_goals',
        'away_team_goals'
    ]
).astype({
    'home_team_id': 'int64',
    'away_team_id': 'int64',
    'home_team_goals': 'int64',
    'away_team_goals': 'int64'
})

print(duckdb.query("""
with c1 as(
select t.team_id,COALESCE(SUM(m.points), 0) as points from teams t left join lateral
( select case when home_team_goals>away_team_goals then 3
                when home_team_goals=away_team_goals then 1
                else 0 end as points
                                
                
                from matches where home_team_id=t.team_id
                
union all
/* guest points*/
select case when home_team_goals<away_team_goals then 3
                when home_team_goals=away_team_goals then 1
                else 0 end from matches  where away_team_id=t.team_id
) m on true
group by t.team_id
order by t.team_id
),

c2 as (
select t.team_id,coalesce(sum(m.total_games)) as total_games from teams t left join lateral
( select home_team_id,count(home_team_id) as Total_games from matches where home_team_id=t.team_id
group by home_team_id
union all
select away_team_id,count(away_team_id) from matches where away_team_id=t.team_id
group by away_team_id
)m on true 
group by team_id
),
c3 as(
select team_id,sum(goal_for) as goal_for from(
select c2.team_id,sum(home_team_goals) as goal_for,  from c2 left join matches m on c2.team_id= m.home_team_id group by team_id
union all
select c2.team_id,sum(away_team_goals) as goal_for,  from c2 left join matches m on c2.team_id= m.away_team_id group by team_id)
group by team_id
),
c4 as(
select team_id,sum(goal_against) goal_against from(
select c2.team_id,sum(away_team_goals) as goal_against,  from c2 left join matches m on c2.team_id= m.home_team_id group by team_id
union all
select c2.team_id,sum(home_team_goals) as goal_against,  from c2 left join matches m on c2.team_id= m.away_team_id group by team_id)
group by team_id)
/*
select team_name,total_games as matches_played,points,goal_for,goal_against,goal_for-goal_against as goal_diff from teams left join c1 using(team_id)
left join c2 using(team_id)
left join c3 using(team_id)
left join c4 using(team_id)
order by points desc,goal_diff desc */

,team_stats AS (
    -- Unpivot: each match produces 2 rows (one per team)
    SELECT 
        home_team_id AS team_id,
        home_team_goals AS goals_for,
        away_team_goals AS goals_against,
        CASE 
            WHEN home_team_goals > away_team_goals THEN 3
            WHEN home_team_goals = away_team_goals THEN 1
            ELSE 0
        END AS points
    FROM matches
    
    UNION ALL
    
    SELECT 
        away_team_id,
        away_team_goals,
        home_team_goals,
        CASE 
            WHEN away_team_goals > home_team_goals THEN 3
            WHEN away_team_goals = home_team_goals THEN 1
            ELSE 0
        END
    FROM matches
)
/*
SELECT 
    t.team_name,
    COALESCE(COUNT(s.team_id), 0) AS matches_played,
    COALESCE(SUM(s.points), 0) AS points,
    COALESCE(SUM(s.goals_for), 0) AS goal_for,
    COALESCE(SUM(s.goals_against), 0) AS goal_against,
    COALESCE(SUM(s.goals_for), 0) - COALESCE(SUM(s.goals_against), 0) AS goal_diff
FROM teams t
LEFT JOIN team_stats s ON t.team_id = s.team_id
GROUP BY t.team_id, t.team_name
ORDER BY points DESC, goal_diff DESC, team_name ASC;

Without CTE */


SELECT 
    t.team_name,
    COALESCE(home.matches + away.matches, 0) AS matches_played,
    COALESCE(home.points + away.points, 0) AS points,
    COALESCE(home.goals_for + away.goals_for, 0) AS goal_for,
    COALESCE(home.goals_against + away.goals_against, 0) AS goal_against,
    COALESCE(home.goals_for + away.goals_for, 0) - COALESCE(home.goals_against + away.goals_against, 0) AS goal_diff
FROM teams t
LEFT JOIN (
    SELECT 
        home_team_id AS team_id,
        COUNT(*) AS matches,
        SUM(CASE 
            WHEN home_team_goals > away_team_goals THEN 3
            WHEN home_team_goals = away_team_goals THEN 1
            ELSE 0
        END) AS points,
        SUM(home_team_goals) AS goals_for,
        SUM(away_team_goals) AS goals_against
    FROM matches
    GROUP BY home_team_id
) home ON t.team_id = home.team_id
LEFT JOIN (
    SELECT 
        away_team_id AS team_id,
        COUNT(*) AS matches,
        SUM(CASE 
            WHEN away_team_goals > home_team_goals THEN 3
            WHEN away_team_goals = home_team_goals THEN 1
            ELSE 0
        END) AS points,
        SUM(away_team_goals) AS goals_for,
        SUM(home_team_goals) AS goals_against
    FROM matches
    GROUP BY away_team_id
) away ON t.team_id = away.team_id
ORDER BY points DESC, goal_diff DESC, team_name ASC;
""").to_df())