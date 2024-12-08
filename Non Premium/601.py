# Row_Number() over()

import pandas as pd
import duckdb


data = [[1, '2017-01-01', 10], [2, '2017-01-02', 109], [3, '2017-01-03', 150], [4, '2017-01-04', 99], [5, '2017-01-05', 145], [6, '2017-01-06', 1455], [7, '2017-01-07', 199], [8, '2017-01-09', 188]]
Stadium = pd.DataFrame(data, columns=['id', 'visit_date', 'people']).astype({'id':'Int64', 'visit_date':'datetime64[ns]', 'people':'Int64'})

print(duckdb.query("""
with cte as(
    select *, id - row_number() over(order by id) as diff
    from Stadium
    where people>=100
)

select id,visit_date,people from cte where diff in
(select diff 
from cte 
group by diff
having count(*)>2)
order by visit_date;
""").to_df())