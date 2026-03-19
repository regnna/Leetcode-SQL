import pandas as pd
import duckdb

accounts=pd.DataFrame([
    [3,108939],
    [2,12747],
    [8,87709],
    [6,91796]
],columns=['account_id','income']).astype({
    'account_id':'int64',
    'income':'int64'
})


print(duckdb.query("""
select 'Low Salary' as category,sum(case when(income<20000) then 1 else 0 end) as accounts_count from accounts 
union
select 'Average Salary' as category,sum(case when(income>=20000 and income<=50000) then 1 else 0 end) as accounts_count from accounts 
union
select 'High Salary' as category,count(income)
/*sum(case when(income>50000) then 1 else 0 end) */
as accounts_count from accounts where income>50000
""").to_df())