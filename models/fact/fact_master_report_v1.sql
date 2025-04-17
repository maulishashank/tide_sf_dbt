with dim_data as (
    SELECT
    dc.unified_company_name as name,
    dc.source_system as system,
    dc.source_company_ref as ref,
    dc.company_status as status
    FROM {{ ref('dim_company') }} dc
), metric_join as (
    SELECT dd.*,
    a.METRIC_1,
    a.METRIC_2,
    a.METRIC_3,
    a.METRIC_4,
    a.METRIC_5
    FROM dim_data dd
    LEFT JOIN {{ ref('stg_alpha') }} a ON dd.ref = a.INTERNAL_COMPANY_REF
)
select * from metric_join