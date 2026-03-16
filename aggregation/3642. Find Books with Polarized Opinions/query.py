import pandas as pd
import duckdb

data = [[1, 'The Great Gatsby', 'F. Scott', 'Fiction', 180], [2, 'To Kill a Mockingbird', 'Harper Lee', 'Fiction', 281], [3, '1984', 'George Orwell', 'Dystopian', 328], [4, 'Pride and Prejudice', 'Jane Austen', 'Romance', 432], [5, 'The Catcher in the Rye', 'J.D. Salinger', 'Fiction', 277]]
books = pd.DataFrame(
    data,
    columns=['book_id', 'title', 'author', 'genre', 'pages']
).astype({
    'book_id': 'Int64',   # nullable integer type
    'title': 'string',
    'author': 'string',
    'genre': 'string',
    'pages': 'Int64'
})
data = [[1, 1, 'Alice', 50, 5], [2, 1, 'Bob', 60, 1], [3, 1, 'Carol', 40, 4], [4, 1, 'David', 30, 2], [5, 1, 'Emma', 45, 5], [6, 2, 'Frank', 80, 4], [7, 2, 'Grace', 70, 4], [8, 2, 'Henry', 90, 5], [9, 2, 'Ivy', 60, 4], [10, 2, 'Jack', 75, 4], [11, 3, 'Kate', 100, 2], [12, 3, 'Liam', 120, 1], [13, 3, 'Mia', 80, 2], [14, 3, 'Noah', 90, 1], [15, 3, 'Olivia', 110, 4], [16, 3, 'Paul', 95, 5], [17, 4, 'Quinn', 150, 3], [18, 4, 'Ruby', 140, 3], [19, 5, 'Sam', 80, 1], [20, 5, 'Tara', 70, 2]]
reading_sessions = pd.DataFrame(data,
columns=['session_id', 'book_id', 'reader_name', 'pages_read', 'session_rating']
).astype({
    'session_id': 'Int64',
    'book_id': 'Int64',
    'reader_name': 'string',
    'pages_read': 'Int64',
    'session_rating': 'Int64'
})


print(duckdb.query("""
with cte as(
select b.book_id,
sum(if(r.session_rating>=4,1,0)) as moreDan,
sum(if(r.session_rating<=2,1,0)) as lessDan,
/*count(r.session_rating>=4) as moreDan, 
count(r.session_rating<=2) as lessDan,*/
count(r.session_rating) as total_reading,
max(r.session_rating)-min(r.session_rating) as rating_spread
,
ROUND(
      1.0 * (SUM(CASE WHEN r.session_rating >= 4 THEN 1 ELSE 0 END)
           + SUM(CASE WHEN r.session_rating <= 2 THEN 1 ELSE 0 END))
      / NULLIF(COUNT(r.session_rating), 0), 3
    ) AS polarization_score


/*(count(r.session_rating>=4)+count(r.session_rating<=2))/count(r.session_rating) as polarization*/

from books b join reading_sessions r on b.book_id=r.book_id
group by b.book_id 
),
cte1 as(
select * from cte c where total_reading>=5 and polarization_score>=0.6 and lessDan>=1 and moreDan>=1
)

select c.book_id,b.title,b.author,b.genre,b.pages,c.rating_spread,c.polarization_score from books b join cte1 c on b.book_id=c.book_id


""").to_df())