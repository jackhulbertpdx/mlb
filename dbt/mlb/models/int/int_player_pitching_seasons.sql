with staging as (
    select * from {{ ref('stg_mlb_team_rosters') }}
),

pitching_stats as (
    select
        player_id,
        team_id,
        season,
        player_team_season_key,
        
        -- Basic stats
        pitching_games,
        pitching_gs,
        pitching_wins,
        pitching_losses,
        pitching_sv,
        pitching_holds,
        pitching_ip,
        pitching_hits,
        pitching_runs,
        pitching_er,
        pitching_hr,
        pitching_bb,
        pitching_so,
        
        -- Rate stats
        pitching_era,
        pitching_whip,
        pitching_avg,
        pitching_k9
        
    from staging
    where (pitching_ip > 0 or pitching_games > 0) and pitching_ip is not null
),

final as (
    select
        *,
        -- Qualification check
        case 
            when pitching_ip >= {{ var('min_pitching_ip') }} then true 
            else false 
        end as is_qualified_pitcher,
        
        -- Role classification
        case
            when pitching_gs / nullif(pitching_games, 0) >= 0.5 then 'Starter'
            when pitching_sv >= 3 then 'Closer'
            else 'Reliever'
        end as pitcher_role
    from pitching_stats
)

select * from final