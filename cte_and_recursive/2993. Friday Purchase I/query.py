import pandas as pd
import duckdb

data = [[11, '2023-11-07', 1126], [15, '2023-11-30', 7473], [17, '2023-11-14', 2414], [12, '2023-11-24', 9692], [8, '2023-11-03', 5117], [1, '2023-11-16', 5241], [10, '2023-11-12', 8266], [13, '2023-11-24', 12000]]
purchases = pd.DataFrame(data, columns=['user_id', 'purchase_date', 'amount_spend']).astype({'user_id':'Int64', 'purchase_date':'datetime64[ns]', 'amount_spend':'Int64'})


print(duckdb.query("""
/*
with cte as(
select *,dayName(purchase_date) dayy,week(purchase_date)-week(date('2023-11-01'))+1 as week_of_month
from purchases
)

select c.week_of_month,c.purchase_date,sum(c.amount_spend) AS total_amount
from cte c 
where c.dayy='Friday' 
group by week_of_month,c.purchase_date

order by week_of_month,c.purchase_date */



/*
with Recursive cte as(
/*Anchor*/
/* get first friday date of November 2023*/
select DATE_ADD(DATE('2023-11-01'),interval((5-dayofweek(DATE('2023-11-01'))+7)%7)DAY) as friday_date
Union

/*Recursion*/
Select Date_add(friday_date,interval 7 day)
from cte

/*Termission Condition*/
where friday_date + INTERVAL 7 DAY <=DATE('2023-11-30')
)


select week(c.friday_date)-week(date('2023-11-01'))+1 as week_of_month ,c.friday_date,sum(p.amount_spend) as total_amount 
from cte c left join purchases p on c.friday_date=p.purchase_date

group by week_of_month,friday_date
having total_amount is not null
order by week_of_month,friday_date*/


""").to_df())