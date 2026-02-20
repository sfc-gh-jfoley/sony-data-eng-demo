{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

with verified as (
    select
        interaction_id,
        fan_id,
        email,
        first_name,
        last_name,
        region,
        country_code,
        event_type,
        event_timestamp,
        title_id,
        device_type,
        'VERIFIED' as account_type,
        raw_id
    from {{ source('silver', 'stg_fan_verified') }}
),

guest as (
    select
        interaction_id,
        null as fan_id,
        null as email,
        null as first_name,
        null as last_name,
        region,
        country_code,
        event_type,
        event_timestamp,
        title_id,
        device_type,
        'GUEST' as account_type,
        raw_id
    from {{ source('silver', 'stg_fan_guest') }}
)

select * from verified
union all
select * from guest
