import pandas as pd
import duckdb

schools = pd.DataFrame([
    [11, 151],
    [5, 48],
    [9, 9],
    [10, 99]
], columns=['school_id', 'capacity']).astype({
    'school_id': 'int64',
    'capacity': 'int64'
})

exam = pd.DataFrame([
    [975, 10],
    [966, 60],
    [844, 76],
    [749, 76],
    [744, 100]
], columns=['score', 'student_count']).astype({
    'score': 'int64',
    'student_count': 'int64'
})

print(duckdb.query("""
/* 1st Solution

with cte as(
select *,rank() over(partition by school_id order by score) as rnk from(
select s.school_id, e.score,s.capacity-e.student_count as diff_count,min(s.capacity-e.student_count) over(partition by school_id) as min_diff_count from schools s left join exam e on e.student_count<=s.capacity
) where diff_count=min_diff_count or score is Null
)


select school_id,coalesce(score,-1) as score   from cte where rnk=1*/


/* 2nd Solution

SELECT 
    s.school_id,
    COALESCE(
        (SELECT e.score 
         FROM exam e 
         WHERE e.student_count <= s.capacity 
         ORDER BY e.student_count DESC, e.score ASC 
         LIMIT 1),
        -1
    ) as score
FROM schools s;
*/

WITH best_match AS (
    SELECT 
        s.school_id,
        e.score,
        ROW_NUMBER() OVER (
            PARTITION BY s.school_id 
            ORDER BY e.student_count DESC, e.score ASC
        ) as rn
    FROM schools s
    JOIN exam e ON e.student_count <= s.capacity
)
SELECT 
    s.school_id,
    COALESCE(bm.score, -1) as score
FROM schools s
LEFT JOIN best_match bm ON s.school_id = bm.school_id AND bm.rn = 1;


""").to_df())