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

`COUNT(*) OVER()` is not the same as `select max(id) from seat`
rather `COUNT(*) OVER()` gives number of rows that very table has like `select count(*) from seat`

case when mod(id,2)=0 then id-1 
      /*when id=(select max(id) from seat)*/
    when COUNT(*) OVER()  and mod(id,2)<>0 then id
          else id+1 end as idd from seat
~~
CASE   WHEN mod(id, 2) <> 0 THEN COALESCE(LEAD(student) OVER(ORDER BY id), student)
        ELSE LAG(student) OVER(ORDER BY id)

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
#### coalesce >> IFNULL()
  COALESCE(c.chargeback_count, 0) AS chargeback_count,
  if chargeback_count is null it will show 0 value
#### Round
```sql
    SELECT ROUND(123.4567, 2);		123.46
    SELECT ROUND(123.4, 0);		123
    SELECT ROUND(99.9);	100
    SELECT ROUND(123.45, -2);		100
    SELECT ROUND(150.75, 0);		151.00
  ROUND(
      1.0 * (SUM(CASE WHEN r.session_rating >= 4 THEN 1 ELSE 0 END)
           + SUM(CASE WHEN r.session_rating <= 2 THEN 1 ELSE 0 END))
      / NULLIF(COUNT(r.session_rating), 0), 3
    ) AS polarization
```

    
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

WINDOW FUNCTIONS:
- Do NOT reduce rows
- Add calculated columns
- Same aggregate repeated per row

GROUP BY:
- Reduces rows
- One row per group
- Aggregation replaces rows


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
### 
```
WINDOW FUNCTIONS[ over(partition by ......)]
├── Aggregate → SUM, AVG, COUNT, MIN, MAX
├── Ranking → ROW_NUMBER, RANK, DENSE_RANK, NTILE,PERCENT_RANK, CUME_DIST
└── Navigation → LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
```

### Type Casting
```sql
select company_id,cast(.74* salary as int) as salaryAfterTax
```
for this scenario we can also used Round()


### concatination with a window function
```sql 
select string_agg(product_name,'' ) within group ( order by product_name) over(partition by customer_id) as purchased_products
from orders
```


column_name=one_column_value  gives us the boolean value if thats true or false
we can use this logic in multiple ways
```sql
select customer_id
from orders 
group by customer_id
having sum(product_name='A')>0 and sum(product_name='B')>0 and sum(product_name='C')=0;
```

### Python>Pandas
'chest_id': 'Int64',   # ✅ nullable integer

Every column in SELECT must be either

- aggregated (MAX, SUM, etc.)
- or listed in GROUP BY


### Just use avg() and group by
rather than using sum()/sum(count(*)) the complex window fun

```sql
select project_id,
/*round(sum(experience_years)/sum(count(*)) over(partition by project_id),2) as Average_years*/
round(avg(experience_years),2) as Average_years
from project p inner join employee e using(employee_id) group by project_id
```


```sql

WITH ActivityCounts AS (
    -- We JOIN with Activities to ensure we catch activities with 0 participants
    SELECT a.name AS activity, COUNT(f.id) AS num
    FROM Activities a
    LEFT JOIN Friends f ON a.name = f.activity
    GROUP BY a.name
),
MinMaxCalculations AS (
    SELECT activity, num,
           MAX(num) OVER() AS max_val,
           MIN(num) OVER() AS min_val
    FROM ActivityCounts
)

```


When the task is to pick a row, use ROW_NUMBER / RANK

When the task is to copy a value, use FIRST_VALUE


### over(order by column_name rows between 6 Preceding and current row)
aggregation(column_name)  over(order by column_name rows between 6 Preceding and current row)

count(*) over (
            order by visited_on
            rows between 6 preceding and current row
        ) as window_size

its count row number (1-7)[give 7even if the row is more than 7]

### complex Rank()
  rank() over( order by 
        case when c1.port is null then c2.flys2
            when c2.port is null then c1.flys
            else (c1.flys+c2.flys2) end desc) as total_fly_rnk

### I dont use having clause often 

should have a proper overview where I can use the Having clause
  basically when we want a group by value rather than a window group by and a filter condition on the top of that 

    When to use HAVING
          1. Filtering on Aggregates
                SELECT customer_id, SUM(price)
                FROM Sales
                GROUP BY customer_id
                HAVING SUM(price) > 1000;
          2. Relational Divivsion (need to find an entity that matches all records in another table)
              Find students who attended all required classes 
                SELECT student_id
                FROM Attendance
                GROUP BY student_id
                HAVING COUNT(DISTINCT class_id) = 5; 
          3. Finding Duplicates
              find email addresses that appear more than once in the users table
                Select email FROM Users 
                GROUP BY email
                HAVING COUNT(*) >1;
          4. Condition-based Group Filtering(Filtering a group based on a logic  check)
              Find Projects that have at least one senior employee of experience>10
                  SELECT project_id
                  FROM Project p
                  JOIN Employee e USING(employee_id)
                  GROUP BY project_id
                  HAVING MAX(experience_years) > 10;
the 'Order of Excecution in SQL'

  * From/Join
  * Where(filter the raw rows)
  * Group by(Bundle the rows into groups)
  * HAving(Filter the groups)
  * SELECT(Pick columns)
  * Window Functions(Apply over())
  * Order by (Sort the results)
  

Can't use Over() with Having clause()

We use qualify() in those scenarios(qualify works like having()+ windows over())
```sql
-- Only works in specific DBs like DuckDB or Snowflake or BigQuery
SELECT student
FROM Seat
QUALIFY COUNT(*) OVER() > 1;
```

| Clause      | Works On         | When it executes       | Can use Window Functions? |   |
|-------------|------------------|------------------------|---------------------------|---|
| WHERE       | Individual Rows  | Before Grouping        | No                        |   |
| HAVING      | Group Aggregates | After Grouping         | No                        |   |
| QUALIFY     | Window Results   | After Window Functions | Yes                       |   |
| CTE + WHERE | Anything         | At the end             | Yes                       |   |



The easiest way to force a null is to wrap your query in MAX(). When MAX() is applied to an empty set, it returns NULL.

```sql
SELECT (
    SELECT DISTINCT salary 
    FROM Employee 
    ORDER BY salary DESC 
    LIMIT 1 OFFSET 1
) AS SecondHighestSalary;
```

OFFSET 1: Skips the first (highest) and takes the next one.