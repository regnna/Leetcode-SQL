import pandas as pd
import duckdb

members_data = [
    [9,  "Alice"],
    [11, "Bob"],
    [3,  "Winston"],
    [8,  "Hercy"],
    [1,  "Narihan"]
]

members = pd.DataFrame(
    members_data,
    columns=["member_id", "name"]
).astype({
    "member_id": "int64",
    "name": "string"
})

visits_data = [
    [22, 11, "2021-10-28"],
    [16, 11, "2021-01-12"],
    [18, 9,  "2021-12-10"],
    [19, 3,  "2021-10-19"],
    [12, 11, "2021-03-01"],
    [17, 8,  "2021-05-07"],
    [21, 9,  "2021-05-12"]
]

visits = pd.DataFrame(
    visits_data,
    columns=["visit_id", "member_id", "visit_date"]
).astype({
    "visit_id": "int64",
    "member_id": "int64",
    "visit_date": "datetime64[ns]"
})

purchases_data = [
    [12, 2000],
    [18, 9000],
    [17, 7000]
]

purchases = pd.DataFrame(
    purchases_data,
    columns=["visit_id", "charged_amount"]
).astype({
    "visit_id": "int64",
    "charged_amount": "int64"
})

print(duckdb.query("""
with cte as(
select * from members m left join visits v on m.member_id=v.member_id left join purchases p on v.visit_id=p.visit_id
)

select member_id,name,
/* 100*(count(charged_amount)/count(member_id_1)) as conversion_rate,*/
case WHEN COUNT(visit_id) = 0 THEN 'Bronze'
    when 100*(count(charged_amount)/count(member_id_1))>=80 then 'Dimond'
    when 100*(count(charged_amount)/count(member_id_1))>=50 then 'Gold'
    when 100*(count(charged_amount)/count(member_id_1))<50 then'Silver' end
 as category from cte group by member_id,name
""").to_df())