with teams_distinct as (
    select distinct
        team_id,
        team_name
    from {{ ref('stg_mlb_team_rosters') }}
    where team_id is not null
),

teams_history as (
    select
        team_id,
        min(season) as first_season_in_data,
        max(season) as last_season_in_data
    from {{ ref('stg_mlb_team_rosters') }}
    group by team_id
),

final as (
    select
        t.team_id,
        t.team_name,
        h.first_season_in_data,
        h.last_season_in_data,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from teams_distinct t
    inner join teams_history h on t.team_id = h.team_id
)

select * from final