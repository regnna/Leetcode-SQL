"""
1. used ranking over row_number
2. over(partition by id order by month range between 2 preceding and current row)
Range between and as order by month it will check preceding months such as if the current month is 7 it will check 6 and 5 
row between check preceding rows even if the preceding row having month 4 and 3 
"""

import pandas as pd
import duckdb

data = [[1, 1, 20], [2, 1, 20], [1, 2, 30], [2, 2, 30], [3, 2, 40], [1, 3, 40], [3, 3, 60], [1, 4, 60], [3, 4, 70], [1, 7, 90], [1, 8, 90]]
employee = pd.DataFrame(data, columns=['id', 'month', 'salary']).astype({'id':'Int64', 'month':'Int64', 'salary':'Int64'})


print(duckdb.query("""
with cte as(
select *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY month DESC) AS rank  FROM employee 
)

select id,month, sum(salary) over(partition by id order by month range between 2 preceding and current row) as salary
from cte where rank <>1 order by id, month desc;
""").to_df())