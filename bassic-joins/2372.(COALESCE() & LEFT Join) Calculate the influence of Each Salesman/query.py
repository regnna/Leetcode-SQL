import pandas as pd
import duckdb

salesperson=pd.DataFrame([
    [1,'Alice'],
    [2,'Bob'],
    [3,'Jerry']
],columns=['salesperson_id','name']).astype({
    'salesperson_id':'int64',
    'name':'string'
})

customer=pd.DataFrame([
    [1,1],
    [2,1],
    [3,2]
],columns=['customer_id','salesperson_id']).astype({
    'customer_id':"int64",
    "salesperson_id":"int64"
})

sales=pd.DataFrame([
    [1,2,892],
    [2,1,354],
    [3,3,988],
    [4,3,856]
],columns=['sale_id','customer_id','price']).astype({
    'sale_id':'int64',
    'customer_id':'int64',
    'price':'int64'
})

print(duckdb.query("""

/*
BUSINESS CONTEXT: Sales performance reporting with zero-imputation for inactive reps
TECHNIQUE: LEFT JOIN chain from dimension (salesperson) to facts (sales)
KEY INSIGHT: Always anchor on dimension table to preserve complete entity list
ANTI-PATTERN AVOIDED: Starting from fact table then back-filling misses inactive entities
*/

SELECT 
    sp.salesperson_id,
    sp.name,
    COALESCE(SUM(s.price), 0) AS total_revenue
FROM salesperson sp
LEFT JOIN customer c 
    ON sp.salesperson_id = c.salesperson_id
LEFT JOIN sales s 
    ON c.customer_id = s.customer_id
GROUP BY sp.salesperson_id, sp.name
ORDER BY total_revenue DESC;

""").to_df())