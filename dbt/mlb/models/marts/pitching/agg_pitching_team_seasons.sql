with pitching_facts as (
    select * from {{ ref('fact_pitching_player_seasons') }}
),

team_pitching_aggs as (
    select
        team_id,
        season,
        team_name,
        season_status,
        
        -- Counting stats
        sum(pitching_games) as team_pitching_games,
        sum(pitching_gs) as team_games_started,
        sum(pitching_ip) as team_innings_pitched,
        sum(pitching_wins) as team_wins,
        sum(pitching_losses) as team_losses,
        sum(pitching_sv) as team_saves,
        sum(pitching_holds) as team_holds,
        sum(pitching_hits) as team_hits_allowed,
        sum(pitching_runs) as team_runs_allowed,
        sum(pitching_er) as team_earned_runs_allowed,
        sum(pitching_hr) as team_hr_allowed,
        sum(pitching_bb) as team_walks_allowed,
        sum(pitching_so) as team_strikeouts_recorded,
        
        -- Team rate stats (weighted averages)
        case
            when sum(pitching_ip) = 0 then null
            else (sum(pitching_er) * 9)::double / sum(pitching_ip)
        end as team_era,
        
        case
            when sum(pitching_ip) = 0 then null
            else (sum(pitching_hits) + sum(pitching_bb))::double / sum(pitching_ip)
        end as team_whip,

        -- Team K/9, BB/9, HR/9
        case
            when sum(pitching_ip) = 0 then null
            else (sum(pitching_so) * 9)::double / sum(pitching_ip)
        end as team_k_per_9,
        
        case
            when sum(pitching_ip) = 0 then null
            else (sum(pitching_bb) * 9)::double / sum(pitching_ip)
        end as team_bb_per_9,
        
        case
            when sum(pitching_ip) = 0 then null
            else (sum(pitching_hr) * 9)::double / sum(pitching_ip)
        end as team_hr_per_9,
        
        -- Win percentage
        case
            when (sum(pitching_wins) + sum(pitching_losses)) = 0 then null
            else sum(pitching_wins)::double / (sum(pitching_wins) + sum(pitching_losses))
        end as team_win_pct,
        
        -- Player counts by role
        count(distinct player_id) as total_pitchers_used,
        sum(case when is_qualified_pitcher then 1 else 0 end) as qualified_pitchers,
        sum(case when pitcher_role = 'Starter' then 1 else 0 end) as starters_used,
        sum(case when pitcher_role = 'Closer' then 1 else 0 end) as closers_used,
        sum(case when pitcher_role = 'Reliever' then 1 else 0 end) as relievers_used,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from pitching_facts
    group by team_id, season, team_name, season_status
)

select * from team_pitching_aggs