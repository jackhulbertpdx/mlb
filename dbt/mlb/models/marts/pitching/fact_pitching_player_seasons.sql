with pitching_intermediate as (
    select * from {{ ref('int_player_pitching_seasons') }}
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

-- Calculate league averages for advanced metrics
league_averages as (
    select
        season,
        avg(pitching_era) as league_avg_era,
        avg(pitching_whip) as league_avg_whip
    from pitching_intermediate
    where is_qualified_pitcher = true
    group by season
),

final as (
    select
        p.player_team_season_key,
        p.player_id,
        p.team_id,
        p.season,
        
        -- Dimensional attributes
        pl.full_name as player_name,
        pl.primary_position_code,
        pl.primary_position_name,
        t.team_name,
        s.season_status,
        
        -- Core pitching statistics
        p.pitching_games,
        p.pitching_gs,
        p.pitching_ip,
        p.pitching_wins,
        p.pitching_losses,
        p.pitching_sv,
        p.pitching_holds,
        p.pitching_hits,
        p.pitching_runs,
        p.pitching_er,
        p.pitching_hr,
        p.pitching_bb,
        p.pitching_so,
        
        -- Rate statistics
        p.pitching_era,
        p.pitching_whip,
        p.pitching_avg,
        p.pitching_k9,
        
        -- Advanced metrics using macros
        {{ calc_fip('p.pitching_hr', 'p.pitching_bb', 'p.pitching_so', 'p.pitching_ip') }} as fip,
        {{ calc_k_bb_ratio('p.pitching_so', 'p.pitching_bb') }} as k_bb_ratio,
        {{ calc_rate_per_9('p.pitching_bb', 'p.pitching_ip') }} as bb_per_9,
        {{ calc_rate_per_9('p.pitching_hr', 'p.pitching_ip') }} as hr_per_9,
        {{ calc_rate_per_9('p.pitching_hits', 'p.pitching_ip') }} as h_per_9,
        {{ calc_era_plus('p.pitching_era', 'la.league_avg_era') }} as era_plus,
        {{ calc_whip_plus('p.pitching_whip', 'la.league_avg_whip') }} as whip_plus,
        {{ calc_win_pct('p.pitching_wins', 'p.pitching_losses') }} as win_pct,
        
        -- BABIP for pitchers (simplified)
        case
            when (p.pitching_hits - p.pitching_hr) <= 0 then null
            else (p.pitching_hits - p.pitching_hr)::double / 
                 greatest(1, (p.pitching_hits - p.pitching_hr) + p.pitching_so)
        end as babip_against,
        
        -- Innings per game
        case
            when p.pitching_games = 0 then null
            else p.pitching_ip / p.pitching_games
        end as ip_per_game,
        
        -- Innings per start (for starters)
        case
            when p.pitching_gs = 0 then null
            else p.pitching_ip / p.pitching_gs
        end as ip_per_start,
        
        -- Classification and qualification
        p.pitcher_role,
        p.is_qualified_pitcher,
        
        -- Timestamps
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from pitching_intermediate p
    inner join players pl on p.player_id = pl.player_id
    inner join teams t on p.team_id = t.team_id
    inner join seasons s on p.season = s.season
    left join league_averages la on p.season = la.season
)

select * from final