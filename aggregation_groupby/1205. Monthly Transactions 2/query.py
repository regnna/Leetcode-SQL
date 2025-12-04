""" union 
    COALESCE
"""

import duckdb
import pandas as pd
pd.set_option("display.max_columns",200)

data = [[101, 'US', 'approved', 1000, '2019-05-18'], [102, 'US', 'declined', 2000, '2019-05-19'], [103, 'US', 'approved', 3000, '2019-06-10'], [104, 'US', 'declined', 4000, '2019-06-13'], [105, 'US', 'approved', 5000, '2019-06-15']]
transactions = pd.DataFrame(data, columns=['id', 'country', 'state', 'amount', 'trans_date']).astype({'id':'Int64', 'country':'object', 'state':'object', 'amount':'Int64', 'trans_date':'datetime64[ns]'})
data = [[102, '2019-05-29'], [101, '2019-06-30'], [105, '2019-09-18']]
chargebacks = pd.DataFrame(data, columns=['trans_id', 'trans_date']).astype({'trans_id':'Int64', 'trans_date':'datetime64[ns]'})


print(duckdb.query("""
with cte as
(select 
    /*dateformat(trans_date,'%Y-%m') as month,*/
    strftime(trans_date, '%Y-%m') AS month,
    country,
    sum(If(state='approved',1,0)) as approved_count,
    sum(If(state='approved',amount,0)) as approved_amount
from transactions
group by 
    /*dateformat(trans_date,'%Y-%m')*/
    strftime(trans_date, '%Y-%m')
    ,country
),

cte2 as
(
select strftime(c.trans_date,'%Y-%m') as month,t.country,
count(*) as chargeback_count, sum(amount) as chargeback_amount
from chargebacks c left join transactions t
on c.trans_id=t.id
group by strftime(c.trans_date,'%Y-%m'),t.country
),

/*select t.*,IFNULL(c.chargeback_count,0) as chargeback_count,
ifnull(c.chargeback_amount,0) as chargeback_amount
from cte as t
left join cte2 as c
on t.month=c.month and t.country=c.country

union

select c.month,c.country,IFNULL(t.approved_count,0) as approved_count
IFNULL(t.approved_amount,0) as approved_amount ,c.chargeback_count,c.chargeback_amount
from cte as t
right join cte2 as c
on t.month=c.month and t.country=c.country
 
                ......Ifnull is not working in duckdb

*/

result as
(
SELECT 
   t.*,
    COALESCE(c.chargeback_count, 0) AS chargeback_count,
    COALESCE(c.chargeback_amount, 0) AS chargeback_amount
FROM cte AS t
LEFT JOIN cte2 AS c ON t.month = c.month AND t.country = c.country

UNION 

SELECT 
    c.month,
    c.country,
    COALESCE(t.approved_count, 0) AS approved_count,
    COALESCE(t.approved_amount, 0) AS approved_amount,
    chargeback_count,
    chargeback_amount
FROM cte2 AS c
LEFT JOIN cte AS t ON c.month = t.month AND c.country = t.country
)


select * 
from result
where approved_count+approved_amount+chargeback_count+chargeback_amount>0;
""").to_df())


