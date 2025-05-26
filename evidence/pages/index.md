---
title: MLB
---

# 🏟️ MLB  

Welcome to your comprehensive MLB analytics platform. Explore player statistics, team performance, and advanced metrics across multiple seasons.

## League Overview

```sql league_overview
select 
    season,
    count(distinct team_id) as total_teams,
    round(avg(team_runs_scored), 1) as avg_runs_per_team,
    round(avg(team_era), 2) as avg_team_era,
    max(team_wins) as best_record
from rpt_team_season_stats
group by season
order by season desc
```

```sql player_overview
select 
    season,
    count(distinct player_id) as total_players,
    sum(case when is_qualified_hitter then 1 else 0 end) as qualified_hitters,
    sum(case when is_qualified_pitcher then 1 else 0 end) as qualified_pitchers
from rpt_player_complete_seasons
group by season
order by season desc
```

<div class="grid grid-cols-2 gap-6">

<div>
<h3>League Statistics by Season</h3>
<DataTable data={league_overview} />
</div>

<div>
<h3>Player Counts by Season</h3>
<DataTable data={player_overview} />
</div>

</div>

## Season Filter

<Dropdown 
    name="selected_season" 
    data={league_overview} 
    value="season" 
    defaultValue={2022}
    title="Select Season"
/>
<ButtonGroup name="selected_season" defaultValue="2022">
    <ButtonGroupItem valueLabel="2022" value="2022">2022</ButtonGroupItem>
    <ButtonGroupItem valueLabel="2021" value="2021">2021</ButtonGroupItem>
    <ButtonGroupItem valueLabel="2020" value="2020">2020</ButtonGroupItem>
</ButtonGroup>
## Key Metrics for {inputs.selected_season || 2022}

```sql season_stats
select 
    team_name,
    team_wins,
    team_losses,
    team_win_pct,
    team_runs_scored,
    team_era,
    run_differential
from rpt_team_season_stats
where season = ${inputs.selected_season || 2022}
order by team_wins desc
```

```sql top_team
select 
    team_name,
    team_wins
from rpt_team_season_stats
where season = ${inputs.selected_season || 2022}
order by team_wins desc
limit 1
```

```sql highest_scoring
select 
    team_name,
    team_runs_scored
from rpt_team_season_stats
where season = ${inputs.selected_season || 2022}
order by team_runs_scored desc
limit 1
```

<div class="grid grid-cols-2 gap-4">

<BigValue 
    data={top_team} 
    value="team_wins" 
    title="Best Record"
    fmt="#"
/>

<BigValue 
    data={highest_scoring} 
    value="team_runs_scored" 
    title="Most Runs Scored"
    fmt="#"
/>

</div>

## Team Performance - {inputs.selected_season || 2024}

<BarChart 
    data={season_stats}
    x="team_name"
    y="team_wins"
    y2="team_losses"
    series="team_wins"
    series2="Losses"
    title="Team Wins vs Losses"
    type="stacked"
    sort=true
/>

## Offense vs Defense

<ScatterPlot 
    data={season_stats}
    x="team_runs_scored"
    y="team_era"
    series="team_name"
    title="Runs Scored vs Team ERA"
    xAxisTitle="Runs Scored"
    yAxisTitle="Team ERA"
    yScale="reverse"
/>

## Current Standings

<DataTable 
    data={season_stats}
    search=true
>
    <Column id="team_name" title="Team" />
    <Column id="team_wins" title="W" 
        colorScale=true 
        scaleColor=blue
        colorMin={season_stats.map(d => d.team_wins).reduce((a,b) => Math.min(a,b))}
        colorMax={season_stats.map(d => d.team_wins).reduce((a,b) => Math.max(a,b))}
    />
    <Column id="team_losses" title="L" 
        colorScale=true
        scaleColor=red
        colorMin={season_stats.map(d => d.team_losses).reduce((a,b) => Math.min(a,b))}
        colorMax={season_stats.map(d => d.team_losses).reduce((a,b) => Math.max(a,b))}
    />
    <Column id="team_win_pct" title="Win%" fmt="pct3" 
        colorScale=true
        scaleColor=green
    />
    <Column id="run_differential" title="+/-" 
        colorScale=true
        scaleColor=diverging
        colorMid=0
    />
    <Column id="team_runs_scored" title="Runs" 
        colorScale=true
        scaleColor=blue
    />
    <Column id="team_era" title="ERA" fmt="num2" 
        colorScale=true
        scaleColor=red
        colorDirection=desc
    />
    <Column id="team_ops" title="OPS" fmt="num3" 
        colorScale=true
        scaleColor=green
    />
    <Column id="pythagorean_win_pct" title="Pyth W%" fmt="pct3" 
        colorScale=true
        scaleColor=orange
    />
</DataTable>

## Top Players for {inputs.selected_season || 2022}

```sql top_hitters_preview
select 
    player_name,
    team_name,
    primary_position_name,
    hitting_games,
    hitting_avg,
    hitting_hr,
    hitting_rbi,
    hitting_ops,
    ops_plus,
    woba
from rpt_player_complete_seasons
where season = ${inputs.selected_season || 2022}
  and is_qualified_hitter = true
order by hitting_ops desc nulls last
limit 10
```

```sql top_pitchers_preview
select 
    player_name,
    team_name,
    pitcher_role,
    pitching_games,
    pitching_ip,
    pitching_era,
    era_plus,
    pitching_wins,
    pitching_sv,
    fip
from rpt_player_complete_seasons
where season = ${inputs.selected_season || 2022}
  and is_qualified_pitcher = true
order by era_plus desc nulls last
limit 10
```

<div class="grid grid-cols-2 gap-6">

<div>
<h3>Top Hitters (OPS Leaders)</h3>
<DataTable data={top_hitters_preview}>
    <Column id="player_name" title="Player" />
    <Column id="team_name" title="Team" />
    <Column id="primary_position_name" title="Pos" />
    <Column id="hitting_games" title="G" />
    <Column id="hitting_avg" title="AVG" fmt="num3" 
        colorScale=true
        scaleColor=green
    />
    <Column id="hitting_hr" title="HR" 
        colorScale=true
        scaleColor=orange
    />
    <Column id="hitting_rbi" title="RBI" 
        colorScale=true
        scaleColor=blue
    />
    <Column id="hitting_ops" title="OPS" fmt="num3" 
        colorScale=true
        scaleColor=green
    />
    <Column id="ops_plus" title="OPS+" 
        colorScale=true
        scaleColor=blue
        colorMid=100
    />
    <Column id="woba" title="wOBA" fmt="num3" 
        colorScale=true
        scaleColor=purple
    />
</DataTable>
</div>

<div>
<h3>Top Pitchers (ERA+ Leaders)</h3>
<DataTable data={top_pitchers_preview}>
    <Column id="player_name" title="Player" />
    <Column id="team_name" title="Team" />
    <Column id="pitcher_role" title="Role" />
    <Column id="pitching_games" title="G" />
    <Column id="pitching_ip" title="IP" fmt="num1" />
    <Column id="pitching_era" title="ERA" fmt="num2" 
        colorScale=true
        scaleColor=red
        colorDirection=desc
    />
    <Column id="era_plus" title="ERA+" 
        colorScale=true
        scaleColor=green
        colorMid=100
    />
    <Column id="pitching_wins" title="W" 
        colorScale=true
        scaleColor=blue
    />
    <Column id="pitching_sv" title="SV" 
        colorScale=true
        scaleColor=orange
    />
    <Column id="fip" title="FIP" fmt="num2" 
        colorScale=true
        scaleColor=red
        colorDirection=desc
    />
</DataTable>
</div>

</div>

## Quick Navigation

<div class="grid grid-cols-3 gap-4 mt-8">

<div class="p-4 border rounded-lg text-center hover:bg-gray-50">
    <h3 class="text-lg font-semibold mb-2">👤 Player Analytics</h3>
    <p class="text-sm text-gray-600 mb-3">Individual player statistics and advanced metrics</p>
    <a href="/players" class="inline-block bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700">Explore Players</a>
</div>

<div class="p-4 border rounded-lg text-center hover:bg-gray-50">
    <h3 class="text-lg font-semibold mb-2">🏟️ Team Analytics</h3>
    <p class="text-sm text-gray-600 mb-3">Team performance and comparative analysis</p>
    <a href="/teams" class="inline-block bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700">Explore Teams</a>
</div>

<div class="p-4 border rounded-lg text-center hover:bg-gray-50">
    <h3 class="text-lg font-semibold mb-2">📊 Advanced Metrics</h3>
    <p class="text-sm text-gray-600 mb-3">Sabermetrics and advanced statistical analysis</p>
    <a href="/analytics" class="inline-block bg-purple-600 text-white px-4 py-2 rounded hover:bg-purple-700">Advanced Analytics</a>
</div>

</div>