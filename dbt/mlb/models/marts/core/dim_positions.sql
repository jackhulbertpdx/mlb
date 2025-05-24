with positions_from_roster as (
    select distinct
        position_code,
        position_name
    from {{ ref('stg_mlb_team_rosters') }}
    where position_code is not null
),

positions_from_fielding as (
    select distinct
        fielding_position as position_code,
        null as position_name
    from {{ ref('stg_mlb_team_rosters') }}
    where fielding_position is not null
      and fielding_position not in (
          select position_code from positions_from_roster
      )
),

all_positions as (
    select position_code, position_name from positions_from_roster
    union all
    select position_code, position_name from positions_from_fielding
),

final as (
    select
        position_code,
        coalesce(position_name, position_code) as position_name,
        
        -- Position grouping
        case
            when position_code = 'P' then 'Pitcher'
            when position_code = 'C' then 'Catcher'
            when position_code in ('1B', '2B', '3B', 'SS') then 'Infield'
            when position_code in ('LF', 'CF', 'RF') then 'Outfield'
            when position_code = 'DH' then 'Designated Hitter'
            else 'Other'
        end as position_group,
        
        -- Position order for sorting
        case
            when position_code = 'C' then 1
            when position_code = '1B' then 2
            when position_code = '2B' then 3
            when position_code = '3B' then 4
            when position_code = 'SS' then 5
            when position_code = 'LF' then 6
            when position_code = 'CF' then 7
            when position_code = 'RF' then 8
            when position_code = 'DH' then 9
            when position_code = 'P' then 10
            else 99
        end as position_sort_order,
        
        current_localtimestamp() as dbt_created_at,
        current_localtimestamp() as dbt_updated_at
        
    from all_positions
)

select * from final