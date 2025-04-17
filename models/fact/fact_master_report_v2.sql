with dim_data as (
    SELECT distinct
    dc.unified_company_name as name,
    dc.target_system as system,
    dc.target_company_ref as ref,
    dc.company_status as status
    FROM {{ ref('dim_company') }} dc
), metric_join as (
    SELECT dd.*,
    b.METRIC_1,
    b.METRIC_2,
    b.METRIC_3,
    b.METRIC_4,
    b.METRIC_5
    FROM dim_data dd
    LEFT JOIN {{ ref('stg_bravo') }} b ON dd.ref = b.INTERNAL_COMPANY_REF
)
select * from metric_join