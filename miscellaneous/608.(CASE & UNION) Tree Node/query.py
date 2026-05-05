import pandas as pd
import duckdb

tree_data = [
    [1, None],
    [2, 1],
    [3, 1],
    [4, 2],
    [5, 2],
    [6,3],
]

tree = pd.DataFrame(
    tree_data,
    columns=["id", "p_id"]
).astype({
    "id": "int64",
    "p_id": "Int64"  # nullable because NULL exists (unlike your patience)
})

print(duckdb.query("""
/*select id,'Root' astype from tree where p_id is NULL
union
select distinct id,'Inner' as type from(
select t1.id,t2.id as child from tree t1 inner join tree t2 on t1.id=t2.p_id where t1.p_id is not NULL)
union
select distinct id, 'Leaf' as type from(
select t1.id,t2.id as child from tree t1 left join tree t2 on t1.id=t2.p_id where t1.p_id is not NULL) where child is NULL*/


SELECT 
    id,
    CASE 
        WHEN p_id IS NULL THEN 'Root'
        WHEN id IN (SELECT p_id FROM tree WHERE p_id IS NOT NULL) THEN 'Inner'
        ELSE 'Leaf'
    END AS type
FROM tree;
""").to_df())