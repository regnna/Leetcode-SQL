import duckdb
import pandas as pd

data = [[1, 10, '2021-03-15'], [1, 11, '2021-03-15'], [1, 12, '2021-03-15'], [2, 10, '2021-03-15'], [2, 11, '2021-03-15'], [2, 12, '2021-03-15'], [3, 10, '2021-03-15'], [3, 11, '2021-03-15'], [3, 12, '2021-03-15'], [4, 10, '2021-03-15'], [4, 11, '2021-03-15'], [4, 13, '2021-03-15'], [5, 10, '2021-03-16'], [5, 11, '2021-03-16'], [5, 12, '2021-03-16']]
listens = pd.DataFrame(data, columns=['user_id', 'song_id', 'day']).astype({'user_id':'Int64', 'song_id':'Int64', 'day':'datetime64[ns]'})
data = [[1, 2]]
friendship = pd.DataFrame(data, columns=['user1_id', 'user2_id']).astype({'user1_id':'Int64', 'user2_id':'Int64'})

print(duckdb.query("""
with cte as(
select l1.user_id as user1_id, l2.user_id  as user2_id
from listens l1 
left join listens l2 on l1.day=l2.day
and l1.song_id=l2.song_id
and l1.user_id<> l2.user_id
group by  l1.day,l1.user_id, l2.user_id
having count(distinct l1.song_id)>=3
)

--select *
select distinct c.user1_id, c.user2_id
from cte c 
left join friendship f 
 on ((c.user1_id=f.user1_id and c.user2_id=f.user2_id)
 or (c.user1_id=f.user2_id and c.user2_id=f.user1_id))
where f.user1_id is null
and c.user1_id is not null
and c.user2_id is not null


--on c.user1_id<>f.user1_id and c.user1_id<>f.user1_id


""").to_df())