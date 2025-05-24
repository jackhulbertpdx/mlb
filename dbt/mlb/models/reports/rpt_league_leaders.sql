with hitting_facts as (
    select * from {{ ref('fact_hitting_player_seasons') }}
),

pitching_facts as (
    select * from {{ ref('fact_pitching_player_seasons') }}
),

hitting_leaders as (
    select
        season,
        'Hitting' as stat_category,
        
        -- Batting Average
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.is_qualified_hitter = true 
           and h2.hitting_avg is not null
         order by h2.hitting_avg desc limit 1) as batting_avg_leader,
        max(case when is_qualified_hitter then hitting_avg else null end) as batting_avg_value,
        
        -- Home Runs
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.hitting_hr is not null
         order by h2.hitting_hr desc limit 1) as home_run_leader,
        max(hitting_hr) as home_run_value,
        
        -- RBIs
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.hitting_rbi is not null
         order by h2.hitting_rbi desc limit 1) as rbi_leader,
        max(hitting_rbi) as rbi_value,
        
        -- Stolen Bases
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.hitting_sb is not null
         order by h2.hitting_sb desc limit 1) as stolen_base_leader,
        max(hitting_sb) as stolen_base_value,
        
        -- OPS
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.is_qualified_hitter = true
           and h2.hitting_ops is not null
         order by h2.hitting_ops desc limit 1) as ops_leader,
        max(case when is_qualified_hitter then hitting_ops else null end) as ops_value,
        
        -- wOBA
        (select player_name from hitting_facts h2 
         where h2.season = h.season 
           and h2.is_qualified_hitter = true
           and h2.woba is not null
         order by h2.woba desc limit 1) as woba_leader,
        max(case when is_qualified_hitter then woba else null end) as woba_value
        
    from hitting_facts h
    group by season
),

pitching_leaders as (
    select
        season,
        'Pitching' as stat_category,
        
        -- ERA
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.is_qualified_pitcher = true
           and p2.pitching_era is not null
         order by p2.pitching_era asc limit 1) as era_leader,
        min(case when is_qualified_pitcher then pitching_era else null end) as era_value,
        
        -- Wins
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.pitching_wins is not null
         order by p2.pitching_wins desc limit 1) as wins_leader,
        max(pitching_wins) as wins_value,
        
        -- Saves
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.pitching_sv is not null
         order by p2.pitching_sv desc limit 1) as saves_leader,
        max(pitching_sv) as saves_value,
        
        -- Strikeouts
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.pitching_so is not null
         order by p2.pitching_so desc limit 1) as strikeout_leader,
        max(pitching_so) as strikeout_value,
        
        -- WHIP
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.is_qualified_pitcher = true
           and p2.pitching_whip is not null
         order by p2.pitching_whip asc limit 1) as whip_leader,
        min(case when is_qualified_pitcher then pitching_whip else null end) as whip_value,
        
        -- FIP
        (select player_name from pitching_facts p2 
         where p2.season = p.season 
           and p2.is_qualified_pitcher = true
           and p2.fip is not null
         order by p2.fip asc limit 1) as fip_leader,
        min(case when is_qualified_pitcher then fip else null end) as fip_value
        
    from pitching_facts p
    group by season
),

final as (
    select
        h.season,
        
        -- Hitting leaders
        h.batting_avg_leader,
        h.batting_avg_value,
        h.home_run_leader,
        h.home_run_value,
        h.rbi_leader,
        h.rbi_value,
        h.stolen_base_leader,
        h.stolen_base_value,
        h.ops_leader,
        h.ops_value,
        h.woba_leader,
        h.woba_value,
        
        -- Pitching leaders
        p.era_leader,
        p.era_value,
        p.wins_leader,
        p.wins_value,
        p.saves_leader,
        p.saves_value,
        p.strikeout_leader,
        p.strikeout_value,
        p.whip_leader,
        p.whip_value,
        p.fip_leader,
        p.fip_value,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from hitting_leaders h
    inner join pitching_leaders p on h.season = p.season
    order by h.season
)

select * from final