import pandas as pd
import duckdb

data = [[1, '2019-07-01', 'mobile', 100], [1, '2019-07-01', 'desktop', 100], [2, '2019-07-01', 'mobile', 100], [2, '2019-07-02', 'mobile', 100], [3, '2019-07-01', 'desktop', 100], [3, '2019-07-02', 'desktop', 100]]
spending = pd.DataFrame(data, columns=['user_id', 'spend_date', 'platform', 'amount']).astype({'user_id':'Int64', 'spend_date':'datetime64[ns]', 'platform':'object', 'amount':'Int64'})

print(duckdb.query("""
with cte as(
select user_id,spend_date,
Group_concat(
    distinct platform order by platform ,',') as p,       
    /*array_agg(distinct platform order by platform) as p,*/
sum(amount) as total 
from spending
group by spend_date,user_id
),

cte2 as
(select spend_date,if(p='desktop,mobile','both',p) as platform, sum(total) as total_amount,count(Distinct user_id) as total_users
from cte group by spend_date,if(p='desktop,mobile','both',p)
),

cte3 as
(select Distinct s.spend_date,j.platform
from spending as s
cross join (select 'mobile' as platform 
    union Select 'desktop'
    union Select 'both')  as j
)
select *
from cte2 union 
select *,0,0
from cte3 
where concat(spend_date,platform ) 
not in (select concat(spend_date,platform) from cte2  )
    ;


""").to_df())