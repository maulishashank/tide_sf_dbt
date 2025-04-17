WITH minus_metrics_a as (
    select INTERNAL_COMPANY_REF from {{ ref('stg_alpha') }}
    minus
    select source_company_ref from {{ ref('dim_company') }}
), minus_metrics_b as (
    select INTERNAL_COMPANY_REF from {{ ref('stg_bravo') }}
    minus
    select distinct target_company_ref from {{ ref('dim_company') }}
), minus_metrics as (
    select * from minus_metrics_a
    UNION
    select * from minus_metrics_b
), metric_join_a as (
    SELECT
    a.COMPANY_NAME as name,
    a.SYSTEM as system,
    a.INTERNAL_COMPANY_REF as ref,
    a.STATUS as status,
    a.METRIC_1,
    a.METRIC_2,
    a.METRIC_3,
    a.METRIC_4,
    a.METRIC_5
    FROM {{ ref('stg_alpha') }} a
    INNER JOIN minus_metrics mm ON mm.INTERNAL_COMPANY_REF = a.INTERNAL_COMPANY_REF
), metric_join_b as (
    SELECT
    b.COMPANY_NAME as name,
    b.SYSTEM as system,
    b.INTERNAL_COMPANY_REF as ref,
    b.STATUS as status,
    b.METRIC_1,
    b.METRIC_2,
    b.METRIC_3,
    b.METRIC_4,
    b.METRIC_5
    FROM {{ ref('stg_bravo') }} b
    INNER JOIN minus_metrics mm ON mm.INTERNAL_COMPANY_REF = b.INTERNAL_COMPANY_REF
)
select * from metric_join_b
UNION
select * from metric_join_a
