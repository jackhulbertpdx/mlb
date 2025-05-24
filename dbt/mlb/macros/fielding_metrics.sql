-- Range Factor per Game
{% macro calc_range_factor_per_game(putouts, assists, games) %}
    case 
        when {{ games }} = 0 then null
        else ({{ putouts }} + {{ assists }})::double / {{ games }}
    end
{% endmacro %}

-- Range Factor per 9 innings
{% macro calc_range_factor_per_9(putouts, assists, innings) %}
    case 
        when {{ innings }} = 0 then null
        else (({{ putouts }} + {{ assists }})::double / {{ innings }}) * 9
    end
{% endmacro %}

-- Fielding Percentage
{% macro calc_fielding_percentage(putouts, assists, errors) %}
    case
        when ({{ putouts }} + {{ assists }} + {{ errors }}) = 0 then null
        else ({{ putouts }} + {{ assists }})::double / ({{ putouts }} + {{ assists }} + {{ errors }})
    end
{% endmacro %}

-- Zone Rating (simplified)
{% macro calc_zone_rating(putouts, assists, chances) %}
    case
        when {{ chances }} = 0 then null
        else ({{ putouts }} + {{ assists }})::double / {{ chances }}
    end
{% endmacro %}