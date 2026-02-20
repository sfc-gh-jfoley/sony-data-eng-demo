{{
    config(
        materialized='table',
        schema='GOLD',
        unique_key='title_id'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['title_id']) }} as title_key,
    title_id,
    title_name,
    franchise,
    release_year,
    genre,
    sub_genre,
    rating,
    runtime_minutes,
    imdb_score,
    budget_usd,
    production_status,
    studio,
    director,
    current_timestamp() as dbt_updated_at
from {{ ref('stg_titles_parsed') }}
