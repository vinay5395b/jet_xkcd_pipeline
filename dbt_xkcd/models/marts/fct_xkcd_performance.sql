select
    comic_id,
    {{ get_comic_cost('title') }} as production_cost_eur,
    (abs(hashtext(comic_id::text)) % 10001) as views_count,
    round((1.0 + (abs(hashtext(comic_id::text || 'review')) % 91) / 10.0)::numeric, 1) as avg_rating
from {{ ref('stg_xkcd__comics') }}