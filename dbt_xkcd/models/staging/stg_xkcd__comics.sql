with source as (
    select * from {{ source('xkcd', 'xkcd_comics') }}
),

renamed as (
    select
        num as comic_id,
        title,
        safe_title,
        img as image_url,
        alt as alt_text,
        transcript,
        news,
        year,
        month,
        day,
        link as external_link,
        ingested_at as loaded_at_utc
    from source
)

select * from renamed