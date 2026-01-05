"""
Lead
Over partition
"""

import duckdb
import pandas as pd

data = [
    [5, 'Smart Crock Pot', '2021-09-18', 698882],
    [6, 'Smart Lock', '2021-09-14', 11487],
    [6, 'Smart Thermostat', '2021-09-10', 674762],
    [8, 'Smart Light Strip', '2021-09-29', 630773],
    [4, 'Smart Cat Feeder', '2021-09-02', 693545],
    [4, 'Smart Bed', '2021-09-13', 170249]
]
users = pd.DataFrame(data, columns=['user_id', 'item', 'created_at', 'amount']).astype({
    'user_id': 'Int64',
    'item': 'object',
    'created_at': 'datetime64[ns]',
    'amount': 'Int64'
})

print(duckdb.query(""" 
with cte as(select *,Lead(created_at,1) 
Over(Partition By user_id order by created_at) as Nxt_Purchase 
from users 
-- where Nxt_Purchase  is not null
)

select Distinct user_id
from cte 
WHERE Nxt_Purchase IS NOT NULL
AND DATEDIFF('day', created_at, Nxt_Purchase) <= 7;
""").to_df())
