with source as (
    select * from {{ source('mlb_raw', 'mlb_team_rosters') }}
),

cleaned as (
    select
        -- Identifiers
        PLAYER_ID as player_id,
        TEAM_ID as team_id,
        SEASON as season,
        
        -- Player info
        FIRST_NAME as first_name,
        LAST_NAME as last_name,
        FULL_NAME as full_name,
        BIRTH_DATE as birth_date,
        BIRTH_CITY as birth_city,
        BIRTH_STATE_PROVINCE as birth_state,
        BIRTH_COUNTRY as birth_country,
        HEIGHT as height,
        WEIGHT as weight,
        BATS as bats,
        THROWS as throws,
        DEBUT_DATE as debut_date,
        
        -- Position info
        POSITION_CODE as position_code,
        POSITION_NAME as position_name,
        PRIMARY_POSITION_CODE as primary_position_code,
        PRIMARY_POSITION_NAME as primary_position_name,
        
        -- Team info
        TEAM_NAME as team_name,
        JERSEY_NUMBER as jersey_number,
        STATUS as player_status,
        
        -- Hitting stats (convert to numeric)
        try_cast(HITTING_GAMES as integer) as hitting_games,
        try_cast(HITTING_AB as integer) as hitting_ab,
        try_cast(HITTING_RUNS as integer) as hitting_runs,
        try_cast(HITTING_HITS as integer) as hitting_hits,
        try_cast(HITTING_DOUBLES as integer) as hitting_doubles,
        try_cast(HITTING_TRIPLES as integer) as hitting_triples,
        try_cast(HITTING_HR as integer) as hitting_hr,
        try_cast(HITTING_RBI as integer) as hitting_rbi,
        try_cast(HITTING_BB as integer) as hitting_bb,
        try_cast(HITTING_SO as integer) as hitting_so,
        try_cast(HITTING_SB as integer) as hitting_sb,
        try_cast(HITTING_CS as integer) as hitting_cs,
        try_cast(HITTING_HBP as integer) as hitting_hbp,
        try_cast(HITTING_IBB as integer) as hitting_ibb,
        try_cast(HITTING_AVG as double) as hitting_avg,
        try_cast(HITTING_OBP as double) as hitting_obp,
        try_cast(HITTING_SLG as double) as hitting_slg,
        try_cast(HITTING_OPS as double) as hitting_ops,
        
        -- Pitching stats
        try_cast(PITCHING_GAMES as integer) as pitching_games,
        try_cast(PITCHING_GS as integer) as pitching_gs,
        try_cast(PITCHING_WINS as integer) as pitching_wins,
        try_cast(PITCHING_LOSSES as integer) as pitching_losses,
        try_cast(PITCHING_SV as integer) as pitching_sv,
        try_cast(PITCHING_HOLDS as integer) as pitching_holds,
        try_cast(PITCHING_IP as double) as pitching_ip,
        try_cast(PITCHING_HITS as integer) as pitching_hits,
        try_cast(PITCHING_RUNS as integer) as pitching_runs,
        try_cast(PITCHING_ER as integer) as pitching_er,
        try_cast(PITCHING_HR as integer) as pitching_hr,
        try_cast(PITCHING_BB as integer) as pitching_bb,
        try_cast(PITCHING_SO as integer) as pitching_so,
        try_cast(PITCHING_ERA as double) as pitching_era,
        try_cast(PITCHING_WHIP as double) as pitching_whip,
        try_cast(PITCHING_AVG as double) as pitching_avg,
        try_cast(PITCHING_K9 as double) as pitching_k9,
        
        -- Fielding stats
        FIELDING_POSITION as fielding_position,
        try_cast(FIELDING_GAMES as integer) as fielding_games,
        try_cast(FIELDING_GS as integer) as fielding_gs,
        try_cast(FIELDING_INNINGS as double) as fielding_innings,
        try_cast(FIELDING_PUTOUTS as integer) as fielding_putouts,
        try_cast(FIELDING_ASSISTS as integer) as fielding_assists,
        try_cast(FIELDING_ERRORS as integer) as fielding_errors,
        try_cast(FIELDING_DOUBLEPLAYS as integer) as fielding_doubleplays,
        try_cast(FIELDING_FIELDINGPERCENTAGE as double) as fielding_percentage,
        
        -- Metadata
        LOADED_TIMESTAMP as loaded_timestamp
        
    from source
),

final as (
    select 
        *,
        -- Generate surrogate keys
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_id', 'season']) }} as player_team_season_key,
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_id', 'season', 'fielding_position']) }} as player_team_season_position_key
    from cleaned
)

select * from final