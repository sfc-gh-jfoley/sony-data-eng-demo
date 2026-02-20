{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

with daily_metrics as (
    select
        title_id,
        report_date,
        theater_region as region,
        sum(tickets_sold) as total_tickets_sold,
        sum(gross_revenue_usd) as total_gross_usd,
        count(distinct theater_id) as theater_count,
        sum(screen_count) as screen_count,
        avg(gross_revenue_usd / nullif(tickets_sold, 0)) as avg_ticket_price_usd
    from {{ ref('stg_box_office_dedup') }}
    group by title_id, report_date, theater_region
),

fan_metrics as (
    select
        title_id,
        event_timestamp::date as activity_date,
        region,
        count(case when event_type = 'STREAM' then 1 end) as stream_count,
        count(distinct fan_id) as unique_fans
    from {{ ref('stg_fans_unified') }}
    group by title_id, event_timestamp::date, region
)

select
    coalesce(d.title_id, f.title_id) as title_id,
    coalesce(d.report_date, f.activity_date) as report_date,
    coalesce(d.region, f.region) as region,
    coalesce(d.total_tickets_sold, 0) as total_tickets_sold,
    coalesce(d.total_gross_usd, 0) as total_gross_usd,
    coalesce(d.theater_count, 0) as theater_count,
    coalesce(d.screen_count, 0) as screen_count,
    coalesce(d.avg_ticket_price_usd, 0) as avg_ticket_price_usd,
    coalesce(f.stream_count, 0) as stream_count,
    coalesce(f.unique_fans, 0) as unique_fans
from daily_metrics d
full outer join fan_metrics f 
    on d.title_id = f.title_id 
    and d.report_date = f.activity_date 
    and d.region = f.region
