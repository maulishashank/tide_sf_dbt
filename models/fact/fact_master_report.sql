SELECT
    dc.company_id,
    cm.source_system,
    COALESCE(b.INTERNAL_COMPANY_REF, a.INTERNAL_COMPANY_REF),
    COALESCE(b.STATUS, a.STATUS),
    COALESCE(b.METRIC_1, a.METRIC_1),
    COALESCE(b.METRIC_2, a.METRIC_2),
    COALESCE(b.METRIC_3, a.METRIC_3),
    COALESCE(b.METRIC_4, a.METRIC_4),
    COALESCE(b.METRIC_5, a.METRIC_5)
FROM {{ ref('dim_company') }} dc
LEFT JOIN {{ ref('stg_alpha') }} a ON cm.source_system = 'A' AND cm.target_company_ref = a.INTERNAL_COMPANY_REF
LEFT JOIN {{ ref('stg_bravo') }} b ON cm.source_system = 'B' AND cm.target_company_ref = b.INTERNAL_COMPANY_REF
LEFT JOIN {{ source('source_lookup_tables', 'customer_mapping') }} cm ON 
--(case when cm.source_system = 'B' then dc.unified_company_name = b.company_name when cm.source_system = 'A' then dc.unified_company_name = a.company_name else dc.unified_company_name = a.company_name end)
dc.unified_company_name =  COALESCE(b.company_name, a.company_name) -- Join on the unified company name
