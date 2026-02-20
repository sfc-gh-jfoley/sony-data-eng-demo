{% snapshot snap_dim_fans %}

{{
    config(
        target_database='SONY_DE',
        target_schema='GOLD',
        unique_key='FAN_ID',
        strategy='check',
        check_cols=['ACCOUNT_TYPE', 'EMAIL_HASH', 'REGION', 'COUNTRY_CODE', 
                    'LAST_ACTIVE_DATE', 'LIFETIME_INTERACTIONS', 'TITLES_ENGAGED', 
                    'PREFERRED_DEVICE'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    FAN_KEY,
    FAN_ID,
    ACCOUNT_TYPE,
    EMAIL,
    EMAIL_HASH,
    FIRST_NAME,
    LAST_NAME,
    REGION,
    COUNTRY_CODE,
    SIGNUP_DATE,
    LAST_ACTIVE_DATE,
    LIFETIME_INTERACTIONS,
    TITLES_ENGAGED,
    PREFERRED_DEVICE,
    EFFECTIVE_FROM
FROM {{ source('gold', 'dt_dim_fans') }}

{% endsnapshot %}
