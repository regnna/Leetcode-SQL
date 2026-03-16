import pandas as pd
import duckdb

data = [[1, 2], [1, 3], [1, 4], [2, 3], [2, 4], [2, 5], [6, 1]]
friendship = pd.DataFrame(data, columns=['user1_id', 'user2_id']).astype({'user1_id':'Int64', 'user2_id':'Int64'})
data = [[1, 88], [2, 23], [3, 24], [4, 56], [5, 11], [6, 33], [2, 77], [3, 77], [6, 88]]
likes = pd.DataFrame(data, columns=['user_id', 'page_id']).astype({'user_id':'Int64', 'page_id':'Int64'})

print(duckdb.query("""
with cte as 
(select * from friendship union select user2_id,user1_id from friendship ),
        -- To find all the friend circles

cte2 as
(select cte.*,l.page_id from cte
left join likes l on cte.user2_id=l.user_id ),
        -- all the pages like by the firends



cte3 as(
select user1_id as user_id,page_id, count(user2_id) as friends_likes
from cte2 group by user1_id, page_id)
        -- how many friends(of the user_id) has liked per page per user_id

select cte3.*
from cte3 
left join likes l2 on
cte3.user_id=l2.user_id
and cte3.page_id=l2.page_id
where l2.page_id is null order by cte3.user_id,cte3.page_id desc;
        -- how many friends(of the user_id) has liked per page per user_id except those pages which are liked by the user itself
""").to_df())