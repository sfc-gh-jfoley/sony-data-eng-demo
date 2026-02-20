{{
    config(
        materialized='table',
        schema='GOLD',
        unique_key='fan_id'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['fan_id']) }} as fan_key,
    fan_id,
    account_type,
    email,
    email_hash,
    first_name,
    last_name,
    region,
    country_code,
    signup_date,
    last_active_date,
    lifetime_interactions,
    titles_engaged,
    preferred_device,
    effective_from,
    current_timestamp() as dbt_updated_at
from {{ ref('int_fans_enriched') }}
