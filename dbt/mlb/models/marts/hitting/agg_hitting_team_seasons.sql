with hitting_facts as (
    select * from {{ ref('fact_hitting_player_seasons') }}
),

team_hitting_aggs as (
    select
        team_id,
        season,
        team_name,
        season_status,
        
        -- Counting stats
        sum(hitting_games) as team_hitting_games,
        sum(hitting_ab) as team_at_bats,
        sum(hitting_pa) as team_plate_appearances,
        sum(hitting_runs) as team_runs_scored,
        sum(hitting_hits) as team_hits,
        sum(hitting_singles) as team_singles,
        sum(hitting_doubles) as team_doubles,
        sum(hitting_triples) as team_triples,
        sum(hitting_hr) as team_home_runs,
        sum(hitting_rbi) as team_rbis,
        sum(hitting_bb) as team_walks,
        sum(hitting_so) as team_strikeouts,
        sum(hitting_sb) as team_stolen_bases,
        sum(hitting_cs) as team_caught_stealing,
        sum(hitting_hbp) as team_hit_by_pitch,
        sum(hitting_tb) as team_total_bases,
        
        -- Team rate stats (weighted averages)
        case
            when sum(hitting_ab) = 0 then null
            else sum(hitting_hits)::double / sum(hitting_ab)
        end as team_batting_avg,
        
        case
            when sum(hitting_pa) = 0 then null
            else (sum(hitting_hits) + sum(hitting_bb) + sum(hitting_hbp))::double / sum(hitting_pa)
        end as team_obp,
        
        case
            when sum(hitting_ab) = 0 then null
            else sum(hitting_tb)::double / sum(hitting_ab)
        end as team_slg,
        
        -- Team OPS
        case
            when sum(hitting_ab) = 0 or sum(hitting_pa) = 0 then null
            else (sum(hitting_hits) + sum(hitting_bb) + sum(hitting_hbp))::double / sum(hitting_pa) +
                 sum(hitting_tb)::double / sum(hitting_ab)
        end as team_ops,
        
        -- Advanced team metrics
        case
            when sum(hitting_pa) = 0 then null
            else sum(hitting_so)::double / sum(hitting_pa)
        end as team_strikeout_rate,
        
        case
            when sum(hitting_pa) = 0 then null
            else sum(hitting_bb)::double / sum(hitting_pa)
        end as team_walk_rate,
        
        case
            when sum(hitting_hits) = 0 then null
            else (sum(hitting_doubles) + sum(hitting_triples) + sum(hitting_hr))::double / sum(hitting_hits)
        end as team_xbh_rate,
        
        -- Player counts
        count(distinct player_id) as total_hitters_used,
        sum(case when is_qualified_hitter then 1 else 0 end) as qualified_hitters,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from hitting_facts
    group by team_id, season, team_name, season_status
)

select * from team_hitting_aggs