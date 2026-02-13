## My Solution
``` sql
(
select name as results from(
select user_id,name,count(user_id) as indie_review, rank() over( order by count(user_id) desc) as rn from movierating join users using(user_id) group by name,user_id) 
where rn=1 order by name asc limit 1)
union 
(select title from(
select distinct title,rank() over(order by avg_rate desc) as rnk from(
select movie_id,title,created_at,avg(rating) over(partition by movie_id) as avg_rate from movierating join movies using(movie_id) where month(created_at)=2 and year(created_at)=2020)
) where rnk=1
order by title asc limit 1
)
```

so my Solution does produces success better habbit:- 
- Never use window AVG() when you need group AVG() — window functions - - - Preserve rows, aggregates collapse them
- Handle tie-breaks in ORDER BY not in subsequent filters
- Minimize nesting — each subquery should have a clear purpose
- Use portable date functions — strftime works in DuckDB, SQLite, PostgreSQL

### Better Solution would be

```sql
WITH user_stats AS (
    SELECT u.name, COUNT(*) as cnt
    FROM movierating mr
    JOIN users u ON mr.user_id = u.user_id
    GROUP BY u.user_id, u.name
),
top_user AS (
    SELECT name, ROW_NUMBER() OVER (ORDER BY cnt DESC, name ASC) as rn
    FROM user_stats
),
feb_movies AS (
    SELECT m.title, AVG(mr.rating) as avg_rate
    FROM movierating mr
    JOIN movies m ON mr.movie_id = m.movie_id
    WHERE strftime(mr.created_at, '%Y-%m') = '2020-02'
    GROUP BY m.movie_id, m.title
),
top_movie AS (
    SELECT title, ROW_NUMBER() OVER (ORDER BY avg_rate DESC, title ASC) as rn
    FROM feb_movies
)
SELECT name AS results FROM top_user WHERE rn = 1
UNION ALL
SELECT title AS results FROM top_movie WHERE rn = 1;
```