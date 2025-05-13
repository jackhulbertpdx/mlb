{{ config(materialized='view') }}

select table_schema, table_name, table_type
from information_schema.tables 
where table_schema = 'MLB_DATA'
order by table_name