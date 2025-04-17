{{ config(materialized='table') }}

with source as (
    select *, 'A' as system
    from {{ source('source_alpha', 'source_a') }}
)

select * from source