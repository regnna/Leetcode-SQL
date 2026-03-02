import pandas as pd
import duckdb

variables=pd.DataFrame([
    ['x',66],
    ['y',77],
],columns=['name','value']).astype({
    "name":'string',
    "value":"int64"
})

expressions=pd.DataFrame([
    ['x','>','y'],
    ['x','<','y'],
    ['x','=','y'],
    ['y','>','x'],
    ['y','<','x'],
    ['x','=','x']
],columns=['left_operand','operator','right_operand']).astype({
    'left_operand':'string',
    'operator':'string',
    'right_operand':'string'
})

print(duckdb.query("""
select left_operand,operator,right_operand ,
/*
case when operator='>' then if(val>val2,'true','false')
     when operator='<' then if(val<val2,'true','false')
     else if(val=val2,'true','false') end
     as value */

CASE
        WHEN (
            (operator = '=' AND v1.value = v2.value)
            OR (operator = '>' AND v1.value > v2.value)
            OR (operator = '<' AND v1.value < v2.value)
        ) THEN 'true'
        ELSE 'false'
    END AS value

from(
select e.*,v.value as val,v1.value as val2 from expressions e left join variables v on e.left_operand=v.name
left join variables v1 on e.right_operand=v1.name
)
""").to_df())
