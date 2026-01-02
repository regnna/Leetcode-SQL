#row_number

import pandas as pd
import duckdb

data=[[1],[2],[3],[7],[8],[10]]
logs=pd.DataFrame(data,columns=['log_id']).astype({'log_id':'Int64'})

print(duckdb.query("""
with cte as(
SELECT
            log_id,
            log_id - ROW_NUMBER() OVER (ORDER BY log_id) AS pid
        FROM logs
)

select min(log_id) as start_id , max(log_id) from cte group by pid

""").to_df())