select
    comic_id,
    safe_title,
    safe_title_alphanumeric_count as title_length,
    production_cost_eur,
    views_count,
    avg_rating
from {{ ref('int_xkcd__comics') }}