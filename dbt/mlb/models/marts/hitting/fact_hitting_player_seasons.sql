with hitting_intermediate as (
    select * from {{ ref('int_player_hitting_seasons') }}
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
        avg(hitting_ops) as league_avg_ops,
        avg(hitting_avg) as league_avg_batting,
        avg(hitting_obp) as league_avg_obp,
        avg(hitting_slg) as league_avg_slg
    from hitting_intermediate
    where is_qualified_hitter = true
    group by season
),

final as (
    select
        h.player_team_season_key,
        h.player_id,
        h.team_id,
        h.season,
        
        -- Dimensional attributes for easier querying
        p.full_name as player_name,
        p.primary_position_code,
        p.primary_position_name,
        t.team_name,
        s.season_status,
        
        -- Core hitting statistics
        h.hitting_games,
        h.hitting_ab,
        h.hitting_pa,
        h.hitting_runs,
        h.hitting_hits,
        h.hitting_singles,
        h.hitting_doubles,
        h.hitting_triples,
        h.hitting_hr,
        h.hitting_rbi,
        h.hitting_bb,
        h.hitting_ibb,
        h.hitting_so,
        h.hitting_sb,
        h.hitting_cs,
        h.hitting_hbp,
        h.hitting_tb,
        
        -- Rate statistics
        h.hitting_avg,
        h.hitting_obp,
        h.hitting_slg,
        h.hitting_ops,
        
        -- Advanced metrics using macros
        {{ calc_iso('h.hitting_slg', 'h.hitting_avg') }} as iso,
        {{ calc_babip('h.hitting_hits', 'h.hitting_hr', 'h.hitting_ab', 'h.hitting_so') }} as babip,
        {{ calc_bb_k_ratio('h.hitting_bb', 'h.hitting_so') }} as bb_k_ratio,
        {{ calc_contact_rate('h.hitting_ab', 'h.hitting_so') }} as contact_rate,
        {{ calc_power_speed_number('h.hitting_hr', 'h.hitting_sb') }} as power_speed_number,
        {{ calc_sb_success_rate('h.hitting_sb', 'h.hitting_cs') }} as sb_success_rate,
        {{ calc_woba('h.hitting_bb', 'h.hitting_hbp', 'h.hitting_singles', 'h.hitting_doubles', 'h.hitting_triples', 'h.hitting_hr', 'h.hitting_pa') }} as woba,
        {{ calc_ops_plus('h.hitting_ops', 'la.league_avg_ops') }} as ops_plus,
        
        -- Extra base hit percentage
        case
            when h.hitting_hits = 0 then null
            else (h.hitting_doubles + h.hitting_triples + h.hitting_hr)::double / h.hitting_hits
        end as xbh_percentage,
        
        -- Qualification and derived fields
        h.is_qualified_hitter,
        
        -- Timestamps
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from hitting_intermediate h
    inner join players p on h.player_id = p.player_id
    inner join teams t on h.team_id = t.team_id
    inner join seasons s on h.season = s.season
    left join league_averages la on h.season = la.season
)

select * from final