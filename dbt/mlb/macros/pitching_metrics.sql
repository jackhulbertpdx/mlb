-- FIP (Fielding Independent Pitching)
{% macro calc_fip(hr, bb, so, ip, league_constant=3.10) %}
    case 
        when {{ ip }} = 0 then null
        else ((13 * {{ hr }}) + (3 * {{ bb }}) - (2 * {{ so }})) / {{ ip }} + {{ league_constant }}
    end
{% endmacro %}

-- K/BB Ratio
{% macro calc_k_bb_ratio(so, bb) %}
    case
        when {{ bb }} = 0 then null
        else {{ so }}::double / {{ bb }}
    end
{% endmacro %}

-- Rate stats per 9 innings
{% macro calc_rate_per_9(stat, ip) %}
    case 
        when {{ ip }} = 0 then null
        else ({{ stat }}::double / {{ ip }}) * 9
    end
{% endmacro %}

-- ERA+ (league adjusted ERA)
{% macro calc_era_plus(player_era, league_avg_era) %}
    case
        when {{ player_era }} = 0 then null
        when {{ league_avg_era }} = 0 then null
        when {{ player_era }} is null then null
        else ({{ league_avg_era }} / {{ player_era }}) * 100
    end
{% endmacro %}

-- WHIP+ (league adjusted WHIP)
{% macro calc_whip_plus(player_whip, league_avg_whip) %}
    case
        when {{ player_whip }} = 0 then null
        when {{ league_avg_whip }} = 0 then null
        when {{ player_whip }} is null then null
        else ({{ league_avg_whip }} / {{ player_whip }}) * 100
    end
{% endmacro %}

-- Win Percentage
{% macro calc_win_pct(wins, losses) %}
    case
        when ({{ wins }} + {{ losses }}) = 0 then null
        else {{ wins }}::double / ({{ wins }} + {{ losses }})
    end
{% endmacro %}