import pandas as pd
import duckdb


queue_data = [
    [5, "Alice", 250, 1],
    [4, "Bob", 175, 5],
    [3, "Alex", 350, 2],
    [6, "John Cena", 400, 3],
    [1, "Winston", 500, 6],
    [2, "Marie", 200, 4]
]

queue = pd.DataFrame(
    queue_data,
    columns=["person_id", "person_name", "weight", "turn"]
).astype({
    "person_id": "int64",
    "person_name": "string",
    "weight": "int64",
    "turn": "int64"
})


print(duckdb.query("""

with cte as(
select * , 
turn-1 as prevtrn, /* I know we dont needed this prev turn colnm but was looking asthetic enough*/
lag(person_name) over( order by turn) as prev_guy, 
sum(weight) over( order by turn) as weight_filled
from queue order by turn
)

/*select prev_guy from cte where weight_filled>1000 limit 1*/


/*select person_name from cte where turn=
(select prevtrn from cte where weight_filled>1000 limit 1)*/

, cte2 AS (
    SELECT 
        person_name, 
        SUM(weight) OVER (ORDER BY turn) AS weight_filled
    FROM queue
)
SELECT person_name
FROM cte2
WHERE weight_filled <= 1000
ORDER BY weight_filled DESC
LIMIT 1;

""").to_df())
