import pandas as pd
import duckdb

keywords_data = [
    [1, "handball"],
    [1, "football"],
    [3, "WAR"],
    [2, "Vaccine"],
]

keywords = pd.DataFrame(
    keywords_data,
    columns=["topic_id", "word"]
).astype({
    "topic_id": "int64",
    "word": "string"
})

posts_data = [
    [1, "We call it soccer They call it football hahaha"],
    [2, "Americans prefer basketball while Europeans love handball and football"],
    [3, "stop the war and play handball"],
    [4, "warning I planted some flowers this morning and then got vaccinated"],
]

posts = pd.DataFrame(
    posts_data,
    columns=["post_id", "content"]
).astype({
    "post_id": "int64",
    "content": "string"
})

print(duckdb.query("""

select post_id,ifnull(group_Concat(distinct topic_id order by topic_id),'Ambiguous!') as topic
from posts as p left join keywords k
on concat(' ',lower(p.content),' ') like concat('% ',lower(word),' %')
group by post_id


""").to_df())