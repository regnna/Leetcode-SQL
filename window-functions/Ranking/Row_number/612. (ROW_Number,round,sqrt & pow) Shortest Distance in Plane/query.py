import pandas as pd
import duckdb

point2d=pd.DataFrame([
    [-1,-1],
    [0,0],
    [-1,-2],
],columns=['x','y']).astype({
    'x':'int64',
    'y':'int64'
})

print(duckdb.query("""
with c as(select *,row_number() over(order by x,y) rn from point2d )
select round(sqrt(pow((p2.x-p1.x),2)+pow((p2.y-p1.y),2)),2) as shortest from c p1  join c p2 on p1.rn>p2.rn order by shortest limit 1

""").to_df())