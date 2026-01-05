import pandas as pd
import duckdb


data = [[1, 501, '2024-01-01', 'start', 'premium', 29.99], [2, 501, '2024-02-15', 'downgrade', 'standard', 19.99], [3, 501, '2024-03-20', 'downgrade', 'basic', 9.99], [4, 502, '2024-01-05', 'start', 'standard', 19.99], [5, 502, '2024-02-10', 'upgrade', 'premium', 29.99], [6, 502, '2024-03-15', 'downgrade', 'basic', 9.99], [7, 503, '2024-01-10', 'start', 'basic', 9.99], [8, 503, '2024-02-20', 'upgrade', 'standard', 19.99], [9, 503, '2024-03-25', 'upgrade', 'premium', 29.99], [10, 504, '2024-01-15', 'start', 'premium', 29.99], [11, 504, '2024-03-01', 'downgrade', 'standard', 19.99], [12, 504, '2024-03-30', 'cancel', None, 0.0], [13, 505, '2024-02-01', 'start', 'basic', 9.99], [14, 505, '2024-02-28', 'upgrade', 'standard', 19.99], [15, 506, '2024-01-20', 'start', 'premium', 29.99], [16, 506, '2024-03-10', 'downgrade', 'basic', 9.99]]
subscription_events=pd.DataFrame(data,columns=["event_id","user_id","event_date","event_type","plan_name","monthly_amount"]).astype({'event_id':'Int64',"user_id":'Int64',"event_date":'datetime64[ns]',"event_type":"object","plan_name":"object","monthly_amount":'Float64'})

print(duckdb.query("""

with cte as(
select *,row_number() over(Partition by user_id order by event_date desc) as rnk from subscription_events
),
cte2 as
(
select user_id, max(case when rnk=1 then plan_name else null end) as current_plan,
sum(case when rnk=1 then monthly_amount else 0 end) as current_monthly_amount,
max(monthly_amount) as max_historical_amount,
date_diff('day',min(event_date),max(event_date)) as days_as_subscriber
from cte
group by user_id
)


select *
from cte2 
where current_monthly_amount<max_historical_amount/2 and
/*current_plan is not NULL and */ 
current_plan<>'Null' and
days_as_subscriber>=60 
order by days_as_subscriber;
""").to_df())