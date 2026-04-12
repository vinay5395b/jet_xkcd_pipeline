with joined as (
    select 
        d.published_at,
        f.production_cost_eur,
        f.views_count
    from {{ ref('dim_xkcd_comics') }} d
    join {{ ref('fct_xkcd_performance') }} f on d.comic_id = f.comic_id
)

select
    date_trunc('month', published_at) as release_month,
    count(*) as total_comics_released,
    sum(production_cost_eur) as total_investment_eur,
    avg(views_count)::int as avg_views_per_comic
from joined
group by 1