import pandas as pd
import duckdb

data = [[1, 'Chelsea', 22, 13, 2, 7], [2, 'Nottingham Forest', 27, 6, 6, 15], [3, 'Liverpool', 17, 1, 8, 8], [4, 'Aston Villa', 20, 1, 6, 13], [5, 'Fulham', 31, 18, 1, 12], [6, 'Burnley', 26, 6, 9, 11], [7, 'Newcastle United', 33, 11, 10, 12], [8, 'Sheffield United', 20, 18, 2, 0], [9, 'Luton Town', 5, 4, 0, 1], [10, 'Everton', 14, 2, 6, 6]]
team_stats = pd.DataFrame(data, columns=["team_id", "team_name", "matches_played", "wins", "draws", "losses"]).astype({"team_id": "int", "team_name": "string", "matches_played": "int", "wins": "int", "draws": "int", "losses": "int"})

print(duckdb.query("""
with cte as(
select team_name, (wins*3+draws*1)as points,rank() over(order by points desc, team_name ) as position,
   from team_stats),

cte2 as
(select Count(team_id) as num_teams from team_stats )

select * , 
case when position < (0.33*(select * from cte2)+1) then 'Tier 1'
     when position > (0.33*(select * from cte2)+1) and position <(0.66*(select * from cte2)+1)  then 'Tier 2'
     else 'Tier 3' end as tier
from cte
order by points desc, team_name;

""").to_df())