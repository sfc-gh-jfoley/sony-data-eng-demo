{{
    config(
        materialized='incremental',
        schema='SILVER',
        unique_key='title_id'
    )
}}

select
    raw_data:title_id::varchar as title_id,
    raw_data:title_name::varchar as title_name,
    raw_data:franchise::varchar as franchise,
    raw_data:release_year::int as release_year,
    raw_data:genre::varchar as genre,
    raw_data:sub_genre::varchar as sub_genre,
    raw_data:rating::varchar as rating,
    raw_data:runtime_minutes::int as runtime_minutes,
    raw_data:imdb_score::float as imdb_score,
    raw_data:budget_usd::number(15,2) as budget_usd,
    raw_data:production_status::varchar as production_status,
    raw_data:studio::varchar as studio,
    raw_data:director::varchar as director,
    raw_id,
    ingestion_ts
from {{ source('bronze', 'raw_title_metadata') }}

{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
