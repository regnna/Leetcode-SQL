import pandas as pd
import duckdb

person_data = [
    [3,  "Jonathan", "051-1234567"],
    [12, "Elvis",    "051-7654321"],
    [1,  "Moncef",   "212-1234567"],
    [2,  "Maroua",   "212-6523651"],
    [7,  "Meir",     "972-1234567"],
    [9,  "Rachel",   "972-0011100"],
]

country_data = [
    ["Peru",     "051"],
    ["Israel",   "972"],
    ["Morocco",  "212"],
    ["Germany",  "049"],
    ["Ethiopia", "251"],
]

calls_data = [
    [1,  9,  33],
    [2,  9,  4],
    [1,  2,  59],
    [3,  12, 102],
    [3,  12, 330],
    [12, 3,  5],
    [7,  9,  13],
    [7,  1,  3],
    [9,  7,  1],
    [1,  7,  7],
]


person = pd.DataFrame(
    person_data,
    columns=["id", "name", "phone_number"]
).astype({
    "id": "int64",
    "name": "string",
    "phone_number": "string"
})

country = pd.DataFrame(
    country_data,
    columns=["name", "country_code"]
).astype({
    "name": "string",
    "country_code": "string"
})

calls = pd.DataFrame(
    calls_data,
    columns=["caller_id", "callee_id", "duration"]
).astype({
    "caller_id": "int64",
    "callee_id": "int64",
    "duration": "int64"
})


print(duckdb.query("""
with c1 as(
select p.*,c.name as country from person p left join country c on substring(p.phone_number,1,3)=c.country_code
),
c2 as(
select distinct id,sum(duration) over(partition by id ) as Sum_by_id,count(id) over(partition by id) as comming_up 
from
(select caller_id as id, duration from calls
union all
select callee_id, duration from calls
)
),
c3 as(
select distinct c1.id,c1.country,sum(sum_by_id) over(partition by country)/sum(comming_up) over(partition by country ) as country_avg ,sum(sum_by_id) over()/sum(comming_up) over() global_avg,c2.Sum_by_id,c2.comming_up from c1 left join c2 using(id)
)

select distinct country from c3 where country_avg>global_avg

""").to_df())