"""
union

"""

import pandas as pd
import duckdb

data = [[1, 2], [1, 3], [2, 3], [1, 4], [2, 4], [1, 5], [2, 5], [1, 7], [3, 7], [1, 6], [3, 6], [2, 6]]
friendship = pd.DataFrame(data, columns=['user1_id', 'user2_id']).astype({'user1_id':'Int64', 'user2_id':'Int64'})


print(duckdb.query("""
with cte as
(select * from friendship
union select user2_id, user1_id from friendship 
)

cte2 as(
select c1.user1_id,c2.user1_id as user2_id,count(c2.user2_id) as common_friend
from cte as c1
left join cte c2 on c1.user2_id=c2.user2_id
and c1.user1_id < c2.user1_id
group by c1.user1_id,c2.user1_id
having count(c2.user2_id)>=3)


select distinct c.*
from cte2 c left join friendship f on c.user1_id=f.user1_id and c.user1_id=f.user1_id 

;

""").to_df())