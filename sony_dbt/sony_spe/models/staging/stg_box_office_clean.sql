{{
    config(
        materialized='incremental',
        schema='SILVER',
        unique_key='record_id'
    )
}}

select
    raw_data:record_id::varchar as record_id,
    raw_data:title_id::varchar as title_id,
    raw_data:report_date::date as report_date,
    raw_data:theater_id::varchar as theater_id,
    raw_data:theater_name::varchar as theater_name,
    raw_data:theater_country::varchar as theater_country,
    raw_data:theater_region::varchar as theater_region,
    raw_data:tickets_sold::int as tickets_sold,
    raw_data:gross_revenue_local::number(15,2) as gross_revenue_local,
    raw_data:local_currency::varchar as local_currency,
    raw_data:exchange_rate_usd::float as exchange_rate_usd,
    raw_data:gross_revenue_usd::number(15,2) as gross_revenue_usd,
    raw_data:screen_count::int as screen_count,
    raw_data:showtime_count::int as showtime_count,
    raw_id,
    ingestion_ts
from {{ source('bronze', 'raw_box_office') }}

{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
