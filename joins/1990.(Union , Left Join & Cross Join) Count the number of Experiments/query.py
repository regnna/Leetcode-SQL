import pandas as pd
import duckdb

experiments_data = [
    [4,  'IOS',      'Programming'],
    [13, 'IOS',      'Sports'],
    [14, 'Android',  'Reading'],
    [8,  'Web',      'Reading'],
    [12, 'Web',      'Reading'],
    [18, 'Web',      'Programming'],
]

experiments = pd.DataFrame(
    experiments_data,
    columns=['experiment_id', 'platform', 'experiment_name']
).astype({
    'experiment_id': 'int64',
    'platform': 'string',
    'experiment_name': 'string'
})

draft=pd.DataFrame(
    [
        ['Android','Reading'],
        ['Android','Sports'],
        ['Android','Programming'],
        ['IOS','Reading'],
        ['IOS','Sports'],
        ['IOS','Programming'],
        ['Web','Reading'],
        ['Web','Sports'],
        ['Web','Programming']
    
        
],columns=['platform','experiment_name']
).astype({
    'platform':'string',
    'experiment_name':'string'
})

print(duckdb.query("""

with cte as(
select * from (
select 'Android' as platform union 
select 'IOS' union 
select 'Web')
t1 cross join (
select 'Programming' as experiment_name union 
select 'Reading' union 
select 'Sports'
) t2
),
cte2 as(
select platform,experiment_name,count(experiment_id) as num_experiments from experiments group by platform,experiment_name)

select c1.platform,c1.experiment_name,coalesce(num_experiments,0) as num_experiments
from cte c1 left join cte2 as c2 on c1.platform=c2.platform and c1.experiment_name=c2.experiment_name


/*select d.platform,d.experiment_name,coalesce(count(experiment_id),0) as num_experiments from draft d left join experiments e on d.platform=e.platform and d.experiment_name=e.experiment_name group by d.platform,d.experiment_name order by d.platform*/

""").to_df())