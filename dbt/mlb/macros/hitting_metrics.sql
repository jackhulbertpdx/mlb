-- Isolated Power
{% macro calc_iso(slg, avg) %}
    case 
        when {{ slg }} is null or {{ avg }} is null then null
        else {{ slg }} - {{ avg }}
    end
{% endmacro %}

-- BABIP (Batting Average on Balls in Play)
{% macro calc_babip(hits, hr, ab, so) %}
    case
        when ({{ ab }} - {{ so }} - {{ hr }}) <= 0 then null
        else ({{ hits }} - {{ hr }})::double / ({{ ab }} - {{ so }} - {{ hr }})
    end
{% endmacro %}

-- BB/K Ratio
{% macro calc_bb_k_ratio(bb, so) %}
    case
        when {{ so }} = 0 then null
        else {{ bb }}::double / {{ so }}
    end
{% endmacro %}

-- Contact Rate
{% macro calc_contact_rate(ab, so) %}
    case
        when {{ ab }} = 0 then null
        else ({{ ab }} - {{ so }})::double / {{ ab }}
    end
{% endmacro %}

-- Power-Speed Number
{% macro calc_power_speed_number(hr, sb) %}
    case
        when ({{ hr }} + {{ sb }}) = 0 then null
        else (2 * {{ hr }} * {{ sb }})::double / ({{ hr }} + {{ sb }})
    end
{% endmacro %}

-- Stolen Base Success Rate
{% macro calc_sb_success_rate(sb, cs) %}
    case
        when ({{ sb }} + {{ cs }}) = 0 then null
        else {{ sb }}::double / ({{ sb }} + {{ cs }})
    end
{% endmacro %}

-- wOBA (simplified version)
{% macro calc_woba(bb, hbp, singles, doubles, triples, hr, pa) %}
    case
        when {{ pa }} = 0 then null
        else (
            0.72 * {{ bb }} + 
            0.75 * {{ hbp }} + 
            0.90 * {{ singles }} + 
            1.25 * {{ doubles }} + 
            1.60 * {{ triples }} + 
            2.05 * {{ hr }}
        )::double / {{ pa }}
    end
{% endmacro %}

-- OPS+ (league adjusted OPS)
{% macro calc_ops_plus(player_ops, league_avg_ops) %}
    case
        when {{ league_avg_ops }} = 0 then null
        when {{ player_ops }} is null then null
        else ({{ player_ops }} / {{ league_avg_ops }}) * 100
    end
{% endmacro %}