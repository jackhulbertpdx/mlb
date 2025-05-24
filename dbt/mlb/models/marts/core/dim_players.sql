with players_latest as (
    select
        player_id,
        first_name,
        last_name,
        full_name,
        birth_date,
        birth_city,
        birth_state,
        birth_country,
        height,
        weight,
        bats,
        throws,
        debut_date,
        primary_position_code,
        primary_position_name,
        
        -- Get most recent data for each player
        row_number() over (
            partition by player_id 
            order by season desc, loaded_timestamp desc
        ) as rn
    from {{ ref('stg_mlb_team_rosters') }}
    where player_id is not null
),

players_career_stats as (
    select
        player_id,
        min(season) as career_first_season,
        max(season) as career_last_season,
        count(distinct season) as seasons_played,
        count(distinct team_id) as teams_played_for
    from {{ ref('stg_mlb_team_rosters') }}
    group by player_id
),

final as (
    select
        p.player_id,
        p.first_name,
        p.last_name,
        p.full_name,
        p.birth_date,
        p.birth_city,
        p.birth_state,
        p.birth_country,
        p.height,
        try_cast(p.weight as integer) as weight,
        p.bats,
        p.throws,
        p.debut_date,
        p.primary_position_code,
        p.primary_position_name,
        
        -- Career info
        c.career_first_season,
        c.career_last_season,
        c.seasons_played,
        c.teams_played_for,
        
        -- Derived fields
        case 
            when extract(year from current_date()) - career_last_season <= 1 then 'Active'
            else 'Retired'
        end as player_status,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from players_latest p
    inner join players_career_stats c on p.player_id = c.player_id
    where p.rn = 1
)

select * from final