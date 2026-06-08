import pandas as pd
import duckdb

seasonstats_data = [
    [2021, 1, "Manchester City",   38, 29, 6, 3, 99, 26],
    [2021, 2, "Liverpool",         38, 28, 8, 2, 94, 26],
    [2021, 3, "Chelsea",           38, 21, 11, 6, 76, 33],
    [2021, 4, "Tottenham",         38, 22, 5, 11, 69, 40],
    [2021, 5, "Arsenal",           38, 22, 3, 13, 61, 48],

    [2022, 1, "Manchester City",   38, 28, 5, 5, 94, 33],
    [2022, 2, "Arsenal",           38, 26, 6, 6, 88, 43],
    [2022, 3, "Manchester United", 38, 23, 6, 9, 58, 43],
    [2022, 4, "Newcastle",         38, 19, 14, 5, 68, 33],
    [2022, 5, "Liverpool",         38, 19, 10, 9, 75, 47],
]

seasonstats = pd.DataFrame(
    seasonstats_data,
    columns=[
        "season_id", "team_id", "team_name",
        "matches_played", "wins", "draws", "losses",
        "goals_for", "goals_against"
    ]
).astype({
    "season_id": "int64",
    "team_id": "int64",
    "team_name": "string",
    "matches_played": "int64",
    "wins": "int64",
    "draws": "int64",
    "losses": "int64",
    "goals_for": "int64",
    "goals_against": "int64"
})

print(duckdb.query("""
with cte as(
select season_id,team_id,team_name,points,goal_difference,rank() over(partition by season_id order by points desc,goal_difference desc,team_name) as position from(
select *,(3*wins+draws) as points,goals_for-goals_against as goal_difference from seasonstats))

select * from cte order by season_id,position,team_name
""").to_df())