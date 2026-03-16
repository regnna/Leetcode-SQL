import pandas as pd
import duckdb


data = [[1, 'Alice Johnson', 'Engineering'], [2, 'Bob Smith', 'Marketing'], [3, 'Carol Davis', 'Sales'], [4, 'David Wilson', 'Engineering'], [5, 'Emma Brown', 'HR']]
employees = pd.DataFrame(data,columns=['employee_id', 'employee_name', 'department']).astype({'employee_id': 'Int64', 'employee_name': 'string', 'department': 'string'})

data = [[1, 1, '2023-06-05', 'Team', 8.0], [2, 1, '2023-06-06', 'Client', 6.0], [3, 1, '2023-06-07', 'Training', 7.0], [4, 1, '2023-06-12', 'Team', 12.0], [5, 1, '2023-06-13', 'Client', 9.0], [6, 2, '2023-06-05', 'Team', 15.0], [7, 2, '2023-06-06', 'Client', 8.0], [8, 2, '2023-06-12', 'Training', 10.0], [9, 3, '2023-06-05', 'Team', 4.0], [10, 3, '2023-06-06', 'Client', 3.0], [11, 4, '2023-06-05', 'Team', 25.0], [12, 4, '2023-06-19', 'Client', 22.0], [13, 5, '2023-06-05', 'Training', 2.0]]
meetings = pd.DataFrame(data,columns=['meeting_id', 'employee_id', 'meeting_date', 'meeting_type', 'duration_hours']).astype({'meeting_id': 'Int64', 'employee_id': 'Int64', 'meeting_date': 'datetime64[ns]', 'meeting_type': 'string', 'duration_hours': 'float64'})


print(duckdb.query("""

with cte as(
select employee_id,STRFTIME(meeting_date, '%V') AS week_number_monday_start
,sum(duration_hours) as total_meeting_time,
(case when sum(duration_hours)/40>0.5 then 'Y' else 'N' end) overworked
/*weekday(date(meeting_date),1) as weks_num*/
from meetings
group by employee_id,STRFTIME(meeting_date, '%V')
)

select c.employee_id,e.employee_name,e.department,sum(if(overworked='Y',1,0)) as meeting_heavy_weeks
from cte as c join employees e on c.employee_id=e.employee_id

group by c.employee_id,e.employee_name,e.department
having sum(if(overworked='Y',1,0))>=2
order by employee_id
;

""").to_df()) 