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
        cast(year || '-' || month || '-' || day as date) as published_at,
        link as external_link,
        -- ingestion timestamp from the source table, renamed for clarity
        ingested_at as loaded_at_utc
    from source
)

select * from renamed