with hitting_aggs as (
    select * from {{ ref('agg_hitting_team_seasons') }}
),

pitching_aggs as (
    select * from {{ ref('agg_pitching_team_seasons') }}
),

teams as (
    select * from {{ ref('dim_teams') }}
),

team_complete_stats as (
    select
        t.team_id,
        t.team_name,
        h.season,
        h.season_status,
        
        -- Record
        p.team_wins,
        p.team_losses,
        p.team_win_pct,
        
        -- Offensive stats
        h.team_runs_scored,
        h.team_hits,
        h.team_home_runs,
        h.team_stolen_bases,
        h.team_batting_avg,
        h.team_obp,
        h.team_slg,
        h.team_ops,
        h.team_strikeout_rate,
        h.team_walk_rate,
        
        -- Pitching stats
        p.team_runs_allowed,
        p.team_era,
        p.team_whip,
        p.team_k_per_9,
        p.team_bb_per_9,
        p.team_hr_per_9,
        p.team_saves,
        
        -- Run differential
        h.team_runs_scored - p.team_runs_allowed as run_differential,
        
        -- Pythagorean Win Expectation
        case
            when (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2)) = 0 then null
            else power(h.team_runs_scored, 2)::double / 
                 (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2))
        end as pythagorean_win_pct,
        
        -- Expected wins (162-game season)
        case
            when (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2)) = 0 then null
            else round(162 * power(h.team_runs_scored, 2)::double / 
                      (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2)))
        end as expected_wins,
        
        -- Win luck (actual wins - expected wins)
        case
            when (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2)) = 0 then null
            else p.team_wins - round(162 * power(h.team_runs_scored, 2)::double / 
                                   (power(h.team_runs_scored, 2) + power(p.team_runs_allowed, 2)))
        end as win_luck,
        
        -- Roster composition
        h.total_hitters_used,
        h.qualified_hitters,
        p.total_pitchers_used,
        p.qualified_pitchers,
        p.starters_used,
        p.closers_used,
        p.relievers_used,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from teams t
    inner join hitting_aggs h on t.team_id = h.team_id
    inner join pitching_aggs p on t.team_id = p.team_id and h.season = p.season
)

select * from team_complete_stats