{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

with fan_stats as (
    select
        fan_id,
        min(event_timestamp) as first_seen,
        max(event_timestamp) as last_seen,
        count(*) as interaction_count,
        count(distinct title_id) as titles_engaged,
        mode(device_type) as preferred_device,
        mode(region) as primary_region,
        mode(country_code) as primary_country
    from {{ ref('stg_fans_unified') }}
    where fan_id is not null
    group by fan_id
),

fan_details as (
    select distinct
        fan_id,
        first_value(email) over (partition by fan_id order by event_timestamp desc) as email,
        first_value(first_name) over (partition by fan_id order by event_timestamp desc) as first_name,
        first_value(last_name) over (partition by fan_id order by event_timestamp desc) as last_name,
        first_value(account_type) over (partition by fan_id order by event_timestamp desc) as account_type
    from {{ ref('stg_fans_unified') }}
    where fan_id is not null
)

select
    d.fan_id,
    d.account_type,
    d.email,
    sha2(d.email, 256) as email_hash,
    d.first_name,
    d.last_name,
    s.primary_region as region,
    s.primary_country as country_code,
    s.first_seen::date as signup_date,
    s.last_seen::date as last_active_date,
    s.interaction_count as lifetime_interactions,
    s.titles_engaged,
    s.preferred_device,
    current_timestamp() as effective_from
from fan_details d
join fan_stats s on d.fan_id = s.fan_id
qualify row_number() over (partition by d.fan_id order by s.last_seen desc) = 1
