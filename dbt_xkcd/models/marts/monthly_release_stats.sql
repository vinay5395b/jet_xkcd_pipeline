with joined as (
    select 
        published_at,
        production_cost_eur,
        views_count
    from {{ ref('int_xkcd__comics') }}
)

select
    date_trunc('month', published_at) as release_month,
    count(*) as total_comics_released,
    sum(production_cost_eur) as total_cost_eur,
    avg(views_count)::int as avg_views_per_comic
from joined
group by 1