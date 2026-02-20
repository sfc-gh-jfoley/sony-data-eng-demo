{{
    config(
        materialized='incremental',
        schema='GOLD',
        unique_key=['title_id', 'report_date', 'region'],
        incremental_strategy='merge'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['title_id', 'report_date', 'region']) }} as performance_key,
    title_id,
    report_date,
    region,
    total_tickets_sold,
    total_gross_usd,
    theater_count,
    screen_count,
    avg_ticket_price_usd,
    stream_count,
    unique_fans,
    current_timestamp() as dbt_updated_at
from {{ ref('int_daily_performance') }}

{% if is_incremental() %}
where report_date >= (select max(report_date) - interval '7 days' from {{ this }})
{% endif %}
