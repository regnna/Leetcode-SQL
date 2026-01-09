import pandas as pd
import duckdb

boxes_data = [
    [2,  None, 6,  15],
    [18, 14,   4,  15],
    [19, 3,    8,  4],
    [12, 2,    19, 20],
    [20, 6,    12, 9],
    [8,  6,    9,  9],
    [3,  14,   16, 7]
]

boxes=pd.DataFrame(boxes_data,columns=['box_id','chest_id','apple_count','orange_count']).astype(
    {
    'box_id':'int64',
        'chest_id': 'Int64',   # ✅ nullable integer
    'apple_count':'int64',
    'orange_count':'int64'
})

# Chests table
chests_data = [
    [6,  5,  6],
    [14, 20, 10],
    [2,  8,  8],
    [3,  19, 4],
    [16, 19, 19]
]

chests = pd.DataFrame(
    chests_data,
    columns=['chest_id', 'apple_count', 'orange_count']
).astype({
    'chest_id': 'int64',
    'apple_count': 'int64',
    'orange_count': 'int64'
})

print(duckdb.query("""

select cast(sum(case when b.chest_id is NULL then b.apple_count
    else c.apple_count+b.apple_count end) as int) as apple_count,
cast(sum(case when b.chest_id is NULL then b.orange_count
    else c.orange_count+b.orange_count end) as int) as orange_count
FROM boxes b
LEFT JOIN chests c
  ON b.chest_id = c.chest_id;


/*select * from boxes  b left join chests c on b.chest_id=c.chest_id*/
/*select sum(b.apple_count+c.apple_count),sum(b.orange_count+c.orange_count) from boxes b left join chests c on b.chest_id=c.chest_id
SELECT
  cast(sum(b.apple_count
    + COALESCE(c.apple_count, 0))as int) AS apple_count,
  cast(sum(b.orange_count
    + COALESCE(c.orange_count, 0))as int) AS orange_count
FROM boxes b
LEFT JOIN chests c
  ON b.chest_id = c.chest_id;
*/
""").to_df())