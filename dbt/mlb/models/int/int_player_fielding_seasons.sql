with staging as (
    select * from {{ ref('stg_mlb_team_rosters') }}
),

fielding_stats as (
    select
        player_id,
        team_id,
        season,
        fielding_position as position_code,
        player_team_season_position_key,
        
        -- Basic stats
        fielding_games,
        fielding_gs,
        fielding_innings,
        fielding_putouts,
        fielding_assists,
        fielding_errors,
        fielding_doubleplays,
        fielding_percentage,
        
        -- Calculated stats
        fielding_putouts + fielding_assists + fielding_errors as fielding_chances
        
    from staging
    where fielding_games > 0 and fielding_position is not null
),

final as (
    select
        *,
        -- Qualification check
        case 
            when fielding_games >= {{ var('min_fielding_games') }} then true 
            else false 
        end as is_qualified_fielder,
        
        -- Position type
        case
            when position_code = 'P' then 'Pitcher'
            when position_code = 'C' then 'Catcher'
            when position_code in ('1B', '2B', '3B', 'SS') then 'Infield'
            when position_code in ('LF', 'CF', 'RF') then 'Outfield'
            when position_code = 'DH' then 'Designated Hitter'
            else 'Unknown'
        end as position_type
    from fielding_stats
)

select * from final