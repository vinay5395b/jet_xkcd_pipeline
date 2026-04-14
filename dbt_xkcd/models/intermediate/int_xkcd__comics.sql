with base as (
    select * from {{ ref('stg_xkcd__comics') }}
),

text_transformations as (
    select
        *,
        {{ get_alphanumeric_count('safe_title') }} as safe_title_alphanumeric_count
    from base
),

comic_costs as (
    select
        *,
        -- Using custom macro for the Cost requirement
        ({{ get_alphanumeric_count('safe_title') }} * 5) as production_cost_eur,

    -- Views: random between 0 and 1 multiplied by 10,000, but deterministic based on comic_id
         (abs(hashtext(comic_id::text)) % 10001) as views_count,

        -- 2. Deterministic Reviews (1.0 to 10.0)
        -- We do the same but scale it to a decimal
        round(
            (1.0 + (abs(hashtext(comic_id::text || 'review')) % 91) / 10.0)::numeric, 1
    ) as avg_rating

    from text_transformations
),

date_transformations as (
    select
        *,
        cast(year || '-' || month || '-' || day as date) as published_at
    from comic_costs
)

select * from date_transformations

