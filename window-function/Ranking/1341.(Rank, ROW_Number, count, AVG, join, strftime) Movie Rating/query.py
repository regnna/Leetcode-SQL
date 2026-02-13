import pandas as pd
import duckdb

# Movies table
movies_data = [
    [1, "Avengers"],
    [2, "Frozen 2"],
    [3, "Joker"],
]
# Users table
users_data = [
    [1, "Daniel"],
    [2, "Monica"],
    [3, "Maria"],
    [4, "James"],
]
# MovieRating table
movierating_data = [
    [1, 1, 3, "2020-01-12"],
    [1, 2, 4, "2020-02-11"],
    [1, 3, 2, "2020-02-12"],
    [1, 4, 1, "2020-01-01"],
    [2, 1, 5, "2020-02-17"],
    [2, 2, 2, "2020-02-01"],
    [2, 3, 2, "2020-03-01"],
    [3, 1, 3, "2020-02-22"],
    [3, 2, 4, "2020-02-25"],
]

movies = pd.DataFrame(
    movies_data,
    columns=["movie_id", "title"]
).astype({
    "movie_id": "int64",
    "title": "string"
})

users = pd.DataFrame(
    users_data,
    columns=["user_id", "name"]
).astype({
    "user_id": "int64",
    "name": "string"
})

movierating = pd.DataFrame(
    movierating_data,
    columns=["movie_id", "user_id", "rating", "created_at"]
).astype({
    "movie_id": "int64",
    "user_id": "int64",
    "rating": "int64",
    "created_at": "datetime64[ns]"  # Will cast to date in SQL
})

print(duckdb.query("""
/*
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
)*/

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

""").to_df())
