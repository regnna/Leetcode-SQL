import pandas as pd
import duckdb

views_data = [
    [1, 3, 5, "2019-08-01"],
    [3, 4, 5, "2019-08-01"],
    [1, 3, 6, "2019-08-02"],
    [2, 7, 7, "2019-08-01"],
    [2, 7, 6, "2019-08-02"],
    [4, 7, 1, "2019-07-22"],
    [3, 4, 4, "2019-07-21"],
    [3, 4, 4, "2019-07-21"],
]

views = pd.DataFrame(
    views_data,
    columns=[
        "article_id",
        "author_id",
        "viewer_id",
        "view_date"
    ]
).astype({
    "article_id": "int64",
    "author_id": "int64",
    "viewer_id": "int64",
    "view_date": "datetime64[ns]"
})


print(duckdb.query("""

/*select viewer_id as id from(*/

select viewer_id,view_date from views group by view_date,viewer_id having count(distinct article_id)>1 order by viewer_id 

/*)group by viewer_id having count(viewer_id)>1 order by viewer_id asc*/
""").to_df())