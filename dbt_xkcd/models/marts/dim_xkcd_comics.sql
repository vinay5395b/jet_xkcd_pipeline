select
    comic_id,
    safe_title,
    alt_text,
    img_url,
    published_at
from {{ ref('stg_xkcd__comics') }}