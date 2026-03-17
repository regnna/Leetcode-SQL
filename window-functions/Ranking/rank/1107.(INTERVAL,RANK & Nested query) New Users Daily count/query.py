import pandas as pd
import duckdb

traffic_data = [
    [1, 'login',    '2019-05-01'],
    [1, 'homepage', '2019-05-01'],
    [1, 'logout',   '2019-05-01'],
    [2, 'login',    '2019-06-21'],
    [2, 'logout',   '2019-06-21'],
    [3, 'login',    '2019-01-01'],
    [3, 'jobs',     '2019-01-01'],
    [3, 'logout',   '2019-01-01'],
    [4, 'login',    '2019-06-21'],
    [4, 'groups',   '2019-06-21'],
    [4, 'logout',   '2019-06-21'],
    [5, 'login',    '2019-03-01'],
    [5, 'logout',   '2019-03-01'],
    [5, 'login',    '2019-06-21'],
    [5, 'logout',   '2019-06-21'],
]

traffic = pd.DataFrame(
    traffic_data,
    columns=['user_id', 'activity', 'activity_date']
).astype({
    'user_id': 'int64',
    'activity': 'string',
    'activity_date': 'datetime64[ns]'
})

print(duckdb.query("""
select activity_date as login_date,count(user_id) as user_count from(
select *, rank() over(partition by user_id order by activity_date asc) as rnk from traffic where activity='login')
where rnk=1 and 
activity_date >= DATE '2019-06-30' - INTERVAL '90 days'
/*activity_date>='2019-04-01'*/
 and activity_date<='2019-06-30' group by activity_date  


""").to_df())
