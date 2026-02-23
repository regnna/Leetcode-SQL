import pandas as pd
import duckdb

purchases=pd.DataFrame([
    [4,2,'2022-03-13'],
    [1,5,'2022-02-11'],
    [3,7,'2022-06-19'],
    [6,2,'2022-03-20'],
    [5,7,'2022-06-19'],
    [2,2,'2022-06-08']
],columns=['purchase_id','user_id','purchase_date']).astype({
    'purchase_id':'int64',
    'user_id':'int64',
    'purchase_date':'datetime64[ns]'
})


print(duckdb.query("""
/*select distinct user_id from
(select user_id, p.purchase_date,q.purchase_date from purchases p join purchases q using(user_id) where p.user_id=q.user_id and p.purchase_id<>q.purchase_id order by user_id)
where datediff('day',p.purchase_date,q.purchase_date)<=7
/*select * from purchases order by user_id
using cross join will create doublet the required rows, o(n²) time complexity*/


SELECT DISTINCT p.user_id
FROM purchases p
JOIN purchases q 
    ON p.user_id = q.user_id 
    AND p.purchase_id < q.purchase_id  -- Only forward, no duplicates
WHERE q.purchase_date <= DATE_ADD(p.purchase_date, INTERVAL 7 DAY)
ORDER BY p.user_id;
*/
WITH next_purchase AS (
    SELECT 
        user_id,
        purchase_date,
        LEAD(purchase_date) OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date,purchase_id
        ) AS next_date
    FROM purchases
)
SELECT DISTINCT user_id
FROM next_purchase
WHERE next_date IS NOT NULL 
  AND next_date <= DATE_ADD(purchase_date, INTERVAL 7 DAY)
ORDER BY user_id;
""").to_df())