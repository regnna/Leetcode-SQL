import pandas as pd
import duckdb

employee_data = [
    [101, 'John',  'A', None],
    [102, 'Dan',   'A', 101],
    [103, 'James', 'A', 101],
    [104, 'Amy',   'A', 101],
    [105, 'Anne',  'A', 101],
    [106, 'Ron',   'B', 102],
    [107, 'James', 'A', 102],
    [108, 'Amy',   'A', 102],
    [109, 'Anne',  'A', 102],
    [110, 'Ron',   'B', 102],
    [111, 'James', 'A', 102],
    [112, 'Amy',   'A', 103],
    [113, 'Anne',  'A', 103],
    [114, 'Ron',   'B', 103],

]

employee = pd.DataFrame(
    employee_data,
    columns=['id', 'name', 'department', 'managerId']
).astype({
    'id': 'int64',
    'name': 'string',
    'department': 'string',
    'managerId': 'Int64'   # nullable integer
})


print(duckdb.query("""
select m.name from employee m left join (
select r.id,count(r.id) as cnt from employee e join employee r on e.managerId=r.id group by r.id) n
on m.id=n.id where n.cnt >=5
""").to_df())
# Use nullable integer for managerId because of NULL values
# employee = employee.