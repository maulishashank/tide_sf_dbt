select * from {{ ref('fact_master_report_v1') }}
UNION
select * from {{ ref('fact_master_report_v2') }}
UNION
select * from {{ ref('fact_master_report_v3') }}