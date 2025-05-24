with hitting as (
    select * from {{ ref('fact_hitting_player_seasons') }}
),

pitching as (
    select * from {{ ref('fact_pitching_player_seasons') }}
),

player_seasons as (
    select
        coalesce(h.player_id, p.player_id) as player_id,
        coalesce(h.team_id, p.team_id) as team_id,
        coalesce(h.season, p.season) as season,
        coalesce(h.player_name, p.player_name) as player_name,
        coalesce(h.team_name, p.team_name) as team_name,
        coalesce(h.primary_position_code, p.primary_position_code) as primary_position_code,
        coalesce(h.primary_position_name, p.primary_position_name) as primary_position_name,
        
        -- Hitting stats
        h.hitting_games,
        h.hitting_ab,
        h.hitting_pa,
        h.hitting_avg,
        h.hitting_obp,
        h.hitting_slg,
        h.hitting_ops,
        h.ops_plus,
        h.hitting_hr,
        h.hitting_rbi,
        h.hitting_sb,
        h.iso,
        h.babip as hitting_babip,
        h.bb_k_ratio as hitting_bb_k_ratio,
        h.woba,
        h.is_qualified_hitter,
        
        -- Pitching stats
        p.pitching_games,
        p.pitching_gs,
        p.pitching_ip,
        p.pitching_era,
        p.pitching_whip,
        p.era_plus,
        p.pitching_wins,
        p.pitching_losses,
        p.pitching_sv,
        p.fip,
        p.k_bb_ratio as pitching_k_bb_ratio,
        p.pitcher_role,
        p.is_qualified_pitcher,
        
        -- Player classification
        case
            when h.player_id is not null and p.player_id is not null then 'Two-Way Player'
            when h.player_id is not null then 'Position Player'
            when p.player_id is not null then 'Pitcher'
            else 'Unknown'
        end as player_type,
        
        -- Primary skill
        case
            when h.is_qualified_hitter and not p.is_qualified_pitcher then 'Hitter'
            when p.is_qualified_pitcher and not h.is_qualified_hitter then 'Pitcher'
            when h.is_qualified_hitter and p.is_qualified_pitcher then 'Two-Way'
            else 'Bench/Utility'
        end as primary_skill
        
    from hitting h
    full outer join pitching p on h.player_id = p.player_id 
                                and h.team_id = p.team_id 
                                and h.season = p.season
),

final as (
    select
        *,
        current_localtimestamp() as dbt_created_at
    from player_seasons
)

select * from final