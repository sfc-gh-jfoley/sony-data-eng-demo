{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

select *
from {{ ref('stg_box_office_clean') }}
qualify row_number() over (
    partition by title_id, report_date, theater_id
    order by ingestion_ts desc
) = 1
