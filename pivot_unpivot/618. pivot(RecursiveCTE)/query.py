import pandas as pd
import duckdb


data = [['Jane', 'America'], ['Pascal', 'Europe'], ['Xi', 'Asia'], ['Jack', 'America']]
student = pd.DataFrame(data, columns=['name', 'continent']).astype({'name':'object', 'continent':'object'})


print(duckdb.query("""

with Recursive cte as(
select *, row_number() over(partition by continent order by name) as rnk
from student
order by continent
),

cte2 as
(select 1 as rnk 
union select rnk+1 from cte2

where rnk <( select max(rnk) from cte)
)



select a.name as America,b.name as Asia,c.name as Europe
from cte2
left join (select rnk,name from cte where continent='America') as  a
on cte2.rnk=a.rnk
left join (select rnk,name from cte where continent='Asia') as  b
on cte2.rnk=b.rnk
left join (select rnk,name from cte where continent='Europe') as  c
on cte2.rnk=c.rnk
order by cte2.rnk

""").to_df())
