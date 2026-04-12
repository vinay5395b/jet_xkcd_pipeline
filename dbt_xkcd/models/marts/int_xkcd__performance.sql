with comics as (
    select * from {{ ref('stg_xkcd__comics') }}
)

select
    comic_id,
    title,
    -- Using custom macro for the Cost requirement
    {{ get_comic_cost('safe_title') }} as production_cost_eur,
    -- Views: random between 0 and 1 multiplied by 10,000
    -- (random() * 10000)::int as views_count,
    -- Reviews: random between 1.0 and 10.0
    -- (random() * (max - min) + min)
    -- round((random() * (10.0 - 1.0) + 1.0)::numeric, 1) as avg_rating,
    (abs(hashtext(comic_id::text)) % 10001) as views_count,

        -- 2. Deterministic Reviews (1.0 to 10.0)
        -- We do the same but scale it to a decimal
        round(
            (1.0 + (abs(hashtext(comic_id::text || 'review')) % 91) / 10.0)::numeric, 1
    ) as avg_rating,
    published_at
from comics