
select
    comic_id,
    safe_title,
    alt_text,
    image_url,
    transcript,
    news,
    external_link,
    published_at,
    loaded_at_utc
from {{ ref('int_xkcd__comics') }}