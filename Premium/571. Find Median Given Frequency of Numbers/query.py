"""
1. we are using recursive common table Expression
2. we have used row_number ranking to get the total numbers on which we have to run the median 
3. "count(*) over() as total_num will" have the total value in every row of that table
4. used case when to mark even and odd case for extracting the median

5. used round and left join basics

"""
import pandas as pd
import duckdb

data = [[0, 7], [1, 1], [2, 3], [3, 1]]
numbers = pd.DataFrame(data, columns=['num', 'frequency']).astype({'num':'Int64', 'frequency':'Int64'})

print(duckdb.query("""

with recursive cte as
(
    --Anchor
    select 1 as Num
    Union all
    --Recursive
    Select Num+1 from cte
    -- Termination 
    Where Num<(select max(frequency) from numbers)
),

cte2 As
(select n.num, row_number() over(order by n.num) as rnk, count(*) over() as total_num
from numbers as n
left join cte as c on n.frequency>=c.Num
order by n.num
),

cte3 as
(select *, 
case 
    when total_num%2=0 then rnk in (total_num/2,(total_num/2)+1)
    else rnk=(total_num+1)/2
end as consider
from cte2 )

select Round(AVG(num),1) as median
from cte3
where consider=True;

""").to_df())