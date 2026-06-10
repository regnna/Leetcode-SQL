import pandas as pd
import duckdb

prompts_data = [
    [1, "Write a blog outline",    120],
    [1, "Generate SQL query",       80],
    [1, "Summarize an article",    200],
    [2, "Create resume bullet",     60],
    [2, "Improve LinkedIn bio",     70],
    [3, "Explain neural networks", 300],
    [3, "Generate interview Q&A",  250],
    [3, "Write cover letter",      180],
    [3, "Optimize Python code",    220]
]

prompts = pd.DataFrame(
    prompts_data,
    columns=["user_id", "prompt", "tokens"]
).astype({
    "user_id": "int64",
    "prompt": "string",
    "tokens": "int64"
})

print(duckdb.query("""

select distinct user_id,prompt_count,round(avg(tokens) over(partition by user_id),2) as avg_tokens
from
(select user_id,count(*) prompt_count from prompts group by user_id having count(*)>2)
left join prompts using(user_id)
order by avg_tokens desc,user_id
""").to_df())