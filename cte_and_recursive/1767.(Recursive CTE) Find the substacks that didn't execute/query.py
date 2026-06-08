import pandas as pd
import duckdb

tasks_data = [
    [1, 3],
    [2, 2],
    [3, 4],
]

tasks = pd.DataFrame(
    tasks_data,
    columns=["task_id", "subtasks_count"]
).astype({
    "task_id": "int64",
    "subtasks_count": "int64"
})


executed_data = [
    [1, 2],
    [3, 1],
    [3, 2],
    [3, 3],
    [3, 4],
]

executed = pd.DataFrame(
    executed_data,
    columns=["task_id", "subtask_id"]
).astype({
    "task_id": "int64",
    "subtask_id": "int64"
})


print(duckdb.query("""

with recurcive cte as(

select task_id,
1 as subtasks
union 
select task_id,subtasks+1
from cte 
where subtasks<(select subtask_count from tasks)
)

select * from cte

""").to_df())