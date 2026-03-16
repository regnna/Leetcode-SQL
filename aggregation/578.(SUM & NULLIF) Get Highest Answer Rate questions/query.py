import pandas as pd
import duckdb

surveyLog=pd.DataFrame([
    [5, 'show',   285, None, 1, 123],
    [5, 'answer', 285, 124124, 1, 124],
    [5, 'show',   369, None, 2, 125],
    [5, 'skip',   369, None, 2, 126],
],columns=['id','action','question_id','answer_id','q_num','timestamp']).astype({
    'id':'int64',
})

print(duckdb.query("""
/*select question_id from(
select question_id,count(answer_id)/count(question_id) answer_rate from surveyLog group by question_id)
order by answer_rate desc,question_id asc limit 1*/


select question_id from surveyLog group by question_id
order by (
    sum(case when action='answer' then 1 end)*1.0/nullif(sum(case when action='show' then 1 end ),0) ) desc
, question_id asc limit 1

/*
SELECT question_id
FROM surveyLog
GROUP BY question_id
ORDER BY 
    SUM(CASE WHEN action = 'answer' THEN 1 ELSE 0 END) * 1.0 
        / NULLIF(SUM(CASE WHEN action = 'show' THEN 1 ELSE 0 END), 0) DESC,
    question_id ASC
LIMIT 1;




SELECT question_id
FROM (
    SELECT 
        question_id,
        SUM(CASE WHEN action = 'answer' THEN 1 ELSE 0 END) * 1.0 
            / SUM(CASE WHEN action = 'show' THEN 1 ELSE 0 END) AS answer_rate
    FROM surveyLog
    GROUP BY question_id
    HAVING SUM(CASE WHEN action = 'show' THEN 1 ELSE 0 END) > 0  -- Avoid div by zero
) 
ORDER BY answer_rate DESC, question_id ASC
LIMIT 1;

select question_id,sum(case when action='answer' then 1 end)*1.0/nullif(sum(case when action='show' then 1 end ),0) from surveyLog group by question_id
*/
""").to_df())