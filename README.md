# Leetcode-SQL
Solving Leetcode Premium Data Science Questions using SQL queries under Python using duckdb and pandas

# precautions
apply single quotes(') to represent the string expression in duchdb query as double quote will try to find column with the same name


pd.set_option("display.max_columns",200)

# Error

<details>
<summary>while selecting columns facing group by error</summary>
When you use GROUP BY, every selected column must be:

- in the GROUP BY, or
- an aggregate (MAX(), MIN(), STRING_AGG(), COUNT…)
  - 
- or an engine-specific exception like ANY_VALUE()

</details>



# Same,same but different
current_plan is not NULL and\
current_plan<>'Null' and

dateformat(trans_date,'%Y-%m') as month,\
    strftime(trans_date, '%Y-%m') AS month,

## Join with same column name
```sql
select ................
from cte c left join 
players p 
using (player_id) / ON p.player_id = c.player_id;
```
### dayofweek()
* friday=5
* saturday=6
* sunday=0
* DATE_ADD(DATE('2023-11-01'),interval((5-dayofweek(DATE('2023-11-01'))+7)%7)DAY) as friday_date :- 
  * it will give us next friday date after '2023-11-01'
* datediff("day",before_date,after_date)

### strftime()
  stringformat time
  %W: Week number of the year, with week 01 starting on the first Monday of the year
  STRFTIME(meeting_date, '%W')
### Week start on monday
weekday(date(meeting_date),1) as weks_num/
week(date(meeting_date),1) as weks_num/
  STRFTIME(meeting_date, '%V') AS week_number_monday_start
  will return week number starting from monday

## Select Aggregation problems
#### Refund_rate 
  sum(case when transaction_type='refund' then 1 else 0 end)/count(distinct transaction_id) as refund_rate
#### if 
  sum(If(state='approved',1,0)) as approved_count,
  sum(If(state='approved',amount,0)) as approved_amount
#### colesce
  COALESCE(c.chargeback_count, 0) AS chargeback_count,
  if chargeback_count is null it will show 0 value
#### Round
  ROUND(
      1.0 * (SUM(CASE WHEN r.session_rating >= 4 THEN 1 ELSE 0 END)
           + SUM(CASE WHEN r.session_rating <= 2 THEN 1 ELSE 0 END))
      / NULLIF(COUNT(r.session_rating), 0), 3
    ) AS polarization
#### Prtition

Rather than doing a group by what can we do is

MAX(price) OVER (PARTITION BY store_id) AS store_max_price,
    MIN(price) OVER (PARTITION BY store_id) AS store_min_price

#### Case
we often use null and a category value in case when statememnt 
we can use any aggregation function on top of that to get only the conditioned value
exp:- MAX(case when chp_rnk=1 then product_name else Null end) MOST_CHP_Product,

and for numerical value we can so a sum where else is 0
exp:- sum(case when chp_rnk=1 then quantity else 0 end) as chp_Qty,

#### STRING_AGG(i.product_name,',')
```sql
SELECT i.store_id, STRING_AGG(i.product_name, ', ') AS max_product_names
  FROM inventory i
  JOIN extremes e ON i.store_id = e.store_id AND i.price = e.store_max_price
  GROUP BY i.store_id
```
#### Complex condition
not for both condition together 
```sql
select order_id,customer_id,order_type from cte where not (order_type=1 and minimum=0)
```

## Group by and over(partition) does the same job
```sql 
select from_id,to_id, sum(duration) from cte group by from_id,to_id

select distinct from_id,to_id, sum(duration) over(partition by from_id,to_id) from cte
```
We need that distinct keyword in over() window function
  * GROUP BY = collapse
  * OVER(PARTITION BY) = decorate
  * Window functions repeat results per row
    * DISTINCT is only there to hide repetition
  * Prefer GROUP BY for final aggregates

## Running total/balance

```sql
select gender, day, 
sum(score_points) over(partition by gender order by gender,day) as total
 from scores


select account_id,day,
SUM(
    CASE 
        WHEN type = 'Deposit' THEN amount 
        ELSE -amount 
    END
) OVER (
    PARTITION BY account_id
    ORDER BY day
) AS balance

 from transactions
```