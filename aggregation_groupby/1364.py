import pandas as pd
import duckdb

data = [[1, 'Alice', 'alice@leetcode.com'], [2, 'Bob', 'bob@leetcode.com'], [13, 'John', 'john@leetcode.com'], [6, 'Alex', 'alex@leetcode.com']]
customers = pd.DataFrame(data, columns=['customer_id', 'customer_name', 'email']).astype({'customer_id':'Int64', 'customer_name':'object', 'email':'object'})
data = [[1, 'Bob', 'bob@leetcode.com'], [1, 'John', 'john@leetcode.com'], [1, 'Jal', 'jal@leetcode.com'], [2, 'Omar', 'omar@leetcode.com'], [2, 'Meir', 'meir@leetcode.com'], [6, 'Alice', 'alice@leetcode.com']]
contacts = pd.DataFrame(data, columns=['user_id', 'contact_name', 'contact_email']).astype({'user_id':'Int64', 'contact_name':'object', 'contact_email':'object'})
data = [[77, 100, 1], [88, 200, 1], [99, 300, 2], [66, 400, 2], [55, 500, 13], [44, 60, 6]]
invoices = pd.DataFrame(data, columns=['invoice_id', 'price', 'user_id']).astype({'invoice_id':'Int64', 'price':'Int64', 'user_id':'Int64'})

print(duckdb.query("""
select i.invoice_id,c.customer_name,i.price,count(con.contact_name) as contacts_cnt,count(c2.customer_name) as trusted_contacts_cnt
from customers c left join invoices i on c.customer_id=i.user_id
left join contacts con on i.user_id=con.user_id
left join customers c2 on con.contact_name=c2.customer_name and con.contact_email=c2.email
group by i.invoice_id,c.customer_name,i.price
order by i.invoice_id
""").to_df())