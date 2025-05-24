with fielding_intermediate as (
    select * from {{ ref('int_player_fielding_seasons') }}
),

players as (
    select * from {{ ref('dim_players') }}
),

teams as (
    select * from {{ ref('dim_teams') }}
),

seasons as (
    select * from {{ ref('dim_seasons') }}
),

positions as (
    select * from {{ ref('dim_positions') }}
),

-- Calculate position averages for normalized metrics
position_averages as (
    select
        season,
        position_code,
        avg(fielding_percentage) as position_avg_fielding_pct,
        avg((fielding_putouts + fielding_assists)::double / fielding_games) as position_avg_range_factor
    from fielding_intermediate
    where is_qualified_fielder = true
    group by season, position_code
),

final as (
    select
        f.player_team_season_position_key,
        f.player_id,
        f.team_id,
        f.season,
        f.position_code,
        
        -- Dimensional attributes
        pl.full_name as player_name,
        t.team_name,
        s.season_status,
        pos.position_name,
        pos.position_group,
        
        -- Core fielding statistics
        f.fielding_games,
        f.fielding_gs,
        f.fielding_innings,
        f.fielding_putouts,
        f.fielding_assists,
        f.fielding_errors,
        f.fielding_doubleplays,
        f.fielding_percentage,
        f.fielding_chances,
        
        -- Advanced metrics using macros
        {{ calc_range_factor_per_game('f.fielding_putouts', 'f.fielding_assists', 'f.fielding_games') }} as range_factor_per_game,
        {{ calc_range_factor_per_9('f.fielding_putouts', 'f.fielding_assists', 'f.fielding_innings') }} as range_factor_per_9,
        {{ calc_zone_rating('f.fielding_putouts', 'f.fielding_assists', 'f.fielding_chances') }} as zone_rating,
        
        -- Normalized metrics (compared to position average)
        case
            when pa.position_avg_fielding_pct = 0 then null
            when f.fielding_percentage is null then null
            else (f.fielding_percentage / pa.position_avg_fielding_pct) * 100
        end as normalized_fielding_pct,
        
        case
            when pa.position_avg_range_factor = 0 then null
            when f.fielding_games = 0 then null
            else (((f.fielding_putouts + f.fielding_assists)::double / f.fielding_games) / pa.position_avg_range_factor) * 100
        end as normalized_range_factor,
        
        -- Rate statistics
        case
            when f.fielding_games = 0 then null
            else f.fielding_errors::double / f.fielding_games
        end as errors_per_game,
        
        case
            when f.fielding_games = 0 then null
            else f.fielding_doubleplays::double / f.fielding_games
        end as double_plays_per_game,
        
        case
            when f.fielding_chances = 0 then null
            else f.fielding_assists::double / f.fielding_chances
        end as assist_rate,
        
        -- Position context
        f.position_type,
        f.is_qualified_fielder,
        
        -- Timestamps
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from fielding_intermediate f
    inner join players pl on f.player_id = pl.player_id
    inner join teams t on f.team_id = t.team_id
    inner join seasons s on f.season = s.season
    inner join positions pos on f.position_code = pos.position_code
    left join position_averages pa on f.season = pa.season and f.position_code = pa.position_code
)

select * from final