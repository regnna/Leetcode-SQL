import pandas as pd
import duckdb

seat_data = [
    [1, "Abbot"],
    [2, "Doris"],
    [3, "Emerson"],
    [4, "Green"],
    [5, "Jeames"],
]

seat = pd.DataFrame(
    seat_data,
    columns=["id", "student"]
).astype({
    "id": "int64",
    "student": "string"
})


print(duckdb.query("""

select id,case when id%2=0 then prev 
when id%2<>0 and nxt is NULL then student
else nxt end as student from(
select *,lag(student) over() as prev, lead(student) over() as nxt from seat
) order by id
""").to_df())