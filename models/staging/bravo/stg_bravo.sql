{{ config(materialized='table') }}

with source as (
    select *, 'B' as system
    from {{ source('source_bravo', 'source_b') }}
)

select * from source