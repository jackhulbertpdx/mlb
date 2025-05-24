with staging as (
    select * from {{ ref('stg_mlb_team_rosters') }}
),

hitting_stats as (
    select
        player_id,
        team_id,
        season,
        player_team_season_key,
        
        -- Basic stats
        hitting_games,
        hitting_ab,
        hitting_runs,
        hitting_hits,
        hitting_doubles,
        hitting_triples,
        hitting_hr,
        hitting_rbi,
        hitting_bb,
        hitting_so,
        hitting_sb,
        hitting_cs,
        hitting_hbp,
        hitting_ibb,
        
        -- Calculated basic stats
        hitting_hits - hitting_doubles - hitting_triples - hitting_hr as hitting_singles,
        hitting_ab + hitting_bb + hitting_hbp as hitting_pa,
        hitting_singles + (2 * hitting_doubles) + (3 * hitting_triples) + (4 * hitting_hr) as hitting_tb,
        
        -- Rate stats
        hitting_avg,
        hitting_obp,
        hitting_slg,
        hitting_ops
        
    from staging
    where (hitting_ab > 0 or hitting_games > 0) and hitting_ab is not null
),

final as (
    select
        *,
        -- Qualification check
        case 
            when hitting_pa >= {{ var('min_hitting_pa') }} then true 
            else false 
        end as is_qualified_hitter
    from hitting_stats
)

select * from final