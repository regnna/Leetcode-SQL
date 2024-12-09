"""
1. used case when
2. if the column name of both the table is same then we can join those tables via `using (column_name)` 
3. Used danse ranking via score in desecending, candidate_id ascending ordr
4. and choosing the rnk rows
"""
import pandas as pd
import duckdb 

# data = [[101, 'Python', 5], [101, 'Tableau', 3], [101, 'PostgreSQL', 4], [101, 'TensorFlow', 2], [102, 'Python', 4], [102, 'Tableau', 5], [102, 'PostgreSQL', 4], [102, 'R', 4], [103, 'Python', 3], [103, 'Tableau', 5], [103, 'PostgreSQL', 5], [103, 'Spark', 4]]
# candidates = pd.DataFrame({
#     'candidate_id': pd.Series(dtype='int'),
#     'skill': pd.Series(dtype='str'),
#     'proficiency': pd.Series(dtype='int')
# })
# data = [[501, 'Python', 4], [501, 'Tableau', 3], [501, 'PostgreSQL', 5], [502, 'Python', 3], [502, 'Tableau', 4], [502, 'R', 2]]
# projects = pd.DataFrame({
#     'project_id': pd.Series(dtype='int'),
#     'skill': pd.Series(dtype='str'),
#     'importance': pd.Series(dtype='int')
# })



# Initialize empty DataFrames with specified column names
candidates = pd.DataFrame(columns=['candidate_id', 'skill', 'proficiency'])
projects = pd.DataFrame(columns=['project_id', 'skill', 'importance'])

# Data to populate into DataFrames
data_candidates = [[101, 'Python', 5], [101, 'Tableau', 3], [101, 'PostgreSQL', 4], [101, 'TensorFlow', 2], 
                   [102, 'Python', 4], [102, 'Tableau', 5], [102, 'PostgreSQL', 4], [102, 'R', 4], 
                   [103, 'Python', 3], [103, 'Tableau', 5], [103, 'PostgreSQL', 5], [103, 'Spark', 4]]

data_projects = [[501, 'Python', 4], [501, 'Tableau', 3], [501, 'PostgreSQL', 5], 
                 [502, 'Python', 3], [502, 'Tableau', 4], [502, 'R', 2]]

# Assign data to the DataFrames
candidates = pd.DataFrame(data_candidates, columns=['candidate_id', 'skill', 'proficiency'])
projects = pd.DataFrame(data_projects, columns=['project_id', 'skill', 'importance'])


print(duckdb.query("""
with cte as 
( select p.project_id,c.candidate_id,
    100+sum(case when c.proficiency>p.importance then 10
    when c.proficiency<p.importance then -5 else 0 end ) as score,
    Count(c.skill) as skill_cnt
    from projects as p
    left join candidates as c
    using(skill)
    group by p.project_id,c.candidate_id
),

/* cte2 as
(
    select *,
    dense_rank() order(partition by project_id order by score desc, candidate_id) as rnk
    from cte
    where (project_id,skill_cnt) in
    (select project_id,count(skill) from projects group by project_id) 
)
cte2 AS (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY project_id ORDER BY score DESC, candidate_id) AS rnk
    FROM cte
    WHERE (project_id, skill_cnt) IN (
        SELECT project_id, COUNT(skill)
        FROM projects
        GROUP BY project_id
    )
)*/

cte2 AS (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY project_id ORDER BY score DESC, candidate_id) AS rnk
    FROM cte
    WHERE project_id IN (
        SELECT project_id
        FROM projects
        GROUP BY project_id
        HAVING COUNT(skill) = skill_cnt
    )
)

Select project_id, candidate_id,score
from cte2
Where rnk =1
Order by project_id


--select * from cte2--
;

""").to_df())

