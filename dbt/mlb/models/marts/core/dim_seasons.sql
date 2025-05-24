with seasons_distinct as (
    select distinct season
    from {{ ref('stg_mlb_team_rosters') }}
    where season is not null
),

final as (
    select
        season,
        season as season_year,
        'MLB ' || season || ' Season' as season_name,
        
        -- Season status
        case
            when season < extract(year from current_date()) then 'Completed'
            when season = extract(year from current_date()) then 'Current'
            else 'Future'
        end as season_status,
        
        -- Decade
        (season // 10) * 10 as decade,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from seasons_distinct
)

select * from final