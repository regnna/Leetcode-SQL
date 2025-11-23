# Leetcode-SQL
Solving Leetcode Premium Data Science Questions using SQL queries under Python using duckdb and pandas

# precautions
apply single quotes(') to represent the string expression in duchdb query as double quote will try to find column with the same name


pd.set_option("display.max_columns",200)


# Same,same but different
current_plan is not NULL and\
current_plan<>'Null' and

dateformat(trans_date,'%Y-%m') as month,\
    strftime(trans_date, '%Y-%m') AS month,

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

