WITH unified_companies AS (
    SELECT
        m.target_system,
        m.target_company_ref,
        m.source_system,
        m.source_company_ref,
        COALESCE(NULLIF(b.COMPANY_NAME, ''), NULLIF(a.COMPANY_NAME, '')) AS unified_company_name,
        COALESCE(b.STATUS, a.STATUS) as company_status -- Give status priority to source B
    FROM {{ source('source_lookup_tables', 'customer_mapping') }} m
    LEFT JOIN {{ ref('stg_alpha') }} a ON m.TARGET_SYSTEM = 'A' AND m.TARGET_COMPANY_REF = a.INTERNAL_COMPANY_REF
    LEFT JOIN {{ ref('stg_bravo') }} b ON m.TARGET_SYSTEM = 'B' AND m.TARGET_COMPANY_REF = b.INTERNAL_COMPANY_REF
    GROUP BY 1,2,3,4,5,6
)
select * from unified_companies