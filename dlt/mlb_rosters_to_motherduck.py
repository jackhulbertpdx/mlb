"""
Script to load data for all MLB teams for the last 5 years (2020-2024) into MotherDuck.
With improved stats retrieval for hitting, pitching, and fielding.
"""
import requests
import pandas as pd
import duckdb
import time
import datetime
import json

# Your MotherDuck token
MOTHERDUCK_TOKEN = "${MOTHERDUCK_TOKEN}"
conn = duckdb.connect(f"md:motherduck_database?motherduck_token={MOTHERDUCK_TOKEN}")

# Define the exact column names from the DDL
columns = [
    # Player identification
    "PLAYER_ID", "TEAM_ID", "TEAM_NAME", "SEASON", "JERSEY_NUMBER",
    
    # Position information
    "POSITION_CODE", "POSITION_NAME", "POSITION_TYPE", "POSITION_ABBREVIATION", "STATUS",
    
    # Player biographical information
    "FIRST_NAME", "LAST_NAME", "FULL_NAME", "PRIMARY_NUMBER", "BIRTH_DATE", 
    "CURRENT_AGE", "BIRTH_CITY", "BIRTH_STATE_PROVINCE", "BIRTH_COUNTRY", "HEIGHT", 
    "WEIGHT", "ACTIVE",
    
    # Player primary position
    "PRIMARY_POSITION_CODE", "PRIMARY_POSITION_NAME", "PRIMARY_POSITION_TYPE", "PRIMARY_POSITION_ABBREVIATION",
    
    # Player characteristics
    "BATS", "THROWS", "DEBUT_DATE", "SERVICE_TIME", "ROOKIE_STATUS",
    
    # Hitting statistics
    "HITTING_AVG", "HITTING_OBP", "HITTING_SLG", "HITTING_OPS", "HITTING_GAMES",
    "HITTING_AB", "HITTING_RUNS", "HITTING_HITS", "HITTING_DOUBLES", "HITTING_TRIPLES",
    "HITTING_HR", "HITTING_RBI", "HITTING_BB", "HITTING_SO", "HITTING_SB",
    "HITTING_CS", "HITTING_HBP", "HITTING_IBB",
    
    # Pitching statistics
    "PITCHING_WINS", "PITCHING_LOSSES", "PITCHING_ERA", "PITCHING_GAMES", "PITCHING_GS",
    "PITCHING_CG", "PITCHING_SHO", "PITCHING_SV", "PITCHING_HOLDS", "PITCHING_IP",
    "PITCHING_HITS", "PITCHING_RUNS", "PITCHING_ER", "PITCHING_HR", "PITCHING_BB",
    "PITCHING_SO", "PITCHING_WHIP", "PITCHING_AVG", "PITCHING_K9",
    
    # Fielding statistics
    "FIELDING_POSITION", "FIELDING_GAMES", "FIELDING_GS", "FIELDING_INNINGS", "FIELDING_CHANCES",
    "FIELDING_PUTOUTS", "FIELDING_ASSISTS", "FIELDING_ERRORS", "FIELDING_DOUBLEPLAYS", 
    "FIELDING_FIELDINGPERCENTAGE",
    
    # Metadata
    "LOADED_TIMESTAMP",
    
    # Additional data
    "ADDITIONAL_DATA"
]

# Function to get MLB teams
def get_mlb_teams(year):
    """Get all active MLB teams for a given year"""
    teams_url = f"https://statsapi.mlb.com/api/v1/teams?season={year}&sportId=1"
    resp = requests.get(teams_url)
    teams_data = resp.json().get("teams", [])
    
    # Filter for MLB teams only (excluding historical teams, etc.)
    mlb_teams = [team for team in teams_data if team.get("sport", {}).get("id") == 1 and team.get("active") == True]
    return mlb_teams

# Function to get player stats directly using separate endpoints for each stat type
def get_player_stats(player_id, year):
    hitting_stats = {}
    pitching_stats = {}
    fielding_stats = {}
    
    # Get hitting stats
    try:
        hitting_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&season={year}&group=hitting&gameType=R"
        hitting_resp = requests.get(hitting_url)
        hitting_data = hitting_resp.json()
        
        if "stats" in hitting_data and hitting_data["stats"] and "splits" in hitting_data["stats"][0] and hitting_data["stats"][0]["splits"]:
            hitting_stats = hitting_data["stats"][0]["splits"][0].get("stat", {})
    except Exception as e:
        print(f"Error getting hitting stats: {e}")
    
    # Get pitching stats
    try:
        pitching_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&season={year}&group=pitching&gameType=R"
        pitching_resp = requests.get(pitching_url)
        pitching_data = pitching_resp.json()
        
        if "stats" in pitching_data and pitching_data["stats"] and "splits" in pitching_data["stats"][0] and pitching_data["stats"][0]["splits"]:
            pitching_stats = pitching_data["stats"][0]["splits"][0].get("stat", {})
    except Exception as e:
        print(f"Error getting pitching stats: {e}")
    
    # Get fielding stats
    try:
        fielding_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&season={year}&group=fielding&gameType=R"
        fielding_resp = requests.get(fielding_url)
        fielding_data = fielding_resp.json()
        
        if "stats" in fielding_data and fielding_data["stats"] and "splits" in fielding_data["stats"][0] and fielding_data["stats"][0]["splits"]:
            # Fielding can have multiple positions, so we should take the primary one
            if len(fielding_data["stats"][0]["splits"]) > 0:
                # Check if player has fielding data for their primary position
                primary_position = None
                for split in fielding_data["stats"][0]["splits"]:
                    if "position" in split.get("stat", {}):
                        position = split.get("stat", {}).get("position", {}).get("code")
                        if position:
                            # If no primary position found yet, use this one
                            if not primary_position:
                                primary_position = position
                                fielding_stats = split.get("stat", {})
                            # If this is the position with most games, use it instead
                            elif split.get("stat", {}).get("games", 0) > fielding_stats.get("games", 0):
                                primary_position = position
                                fielding_stats = split.get("stat", {})
                
                # If no position found yet, just use the first one
                if not fielding_stats and fielding_data["stats"][0]["splits"]:
                    fielding_stats = fielding_data["stats"][0]["splits"][0].get("stat", {})
    except Exception as e:
        print(f"Error getting fielding stats: {e}")
    
    return hitting_stats, pitching_stats, fielding_stats

# Years to fetch (last 5 years including 2024)
years = [2020, 2021, 2022, 2023, 2024]
all_players = []
total_teams_processed = 0
total_players_processed = 0

for year in years:
    print(f"\n===== Processing all MLB teams for year {year} =====")
    
    # Get all teams for this year
    mlb_teams = get_mlb_teams(year)
    print(f"Found {len(mlb_teams)} MLB teams for {year}")
    
    year_players = []
    year_teams_processed = 0
    
    # Process each team
    for team in mlb_teams:
        team_id = team.get("id")
        team_name = team.get("name")
        
        print(f"\nProcessing {team_name} (ID: {team_id}) for year {year}")
        
        # Get roster
        try:
            resp = requests.get(f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?season={year}")
            roster = resp.json().get("roster", [])
            print(f"Found {len(roster)} players for {team_name} in {year}")
            
            if not roster:
                print(f"No roster found for {team_name} in {year}, skipping team")
                continue
            
            team_players = []
            for i, player in enumerate(roster):
                player_id = player.get("person", {}).get("id")
                print(f"Processing player {i+1}/{len(roster)}: {player_id} ({player.get('person', {}).get('fullName', 'Unknown')}) in {year}")
                
                # Get player details
                try:
                    player_resp = requests.get(f"https://statsapi.mlb.com/api/v1/people/{player_id}?season={year}")
                    person = player_resp.json().get("people", [{}])[0]
                    
                    # Get player stats with separate requests for each type
                    hitting_stats, pitching_stats, fielding_stats = get_player_stats(player_id, year)
                    
                    # Create player record
                    player_info = {
                        # Player identification
                        "PLAYER_ID": player_id,
                        "TEAM_ID": team_id,
                        "TEAM_NAME": team_name,
                        "SEASON": year,
                        "JERSEY_NUMBER": player.get("jerseyNumber"),
                        
                        # Position information
                        "POSITION_CODE": player.get("position", {}).get("code"),
                        "POSITION_NAME": player.get("position", {}).get("name"),
                        "POSITION_TYPE": player.get("position", {}).get("type"),
                        "POSITION_ABBREVIATION": player.get("position", {}).get("abbreviation"),
                        "STATUS": player.get("status", {}).get("code"),
                        
                        # Player biographical information
                        "FIRST_NAME": person.get("firstName"),
                        "LAST_NAME": person.get("lastName"),
                        "FULL_NAME": person.get("fullName"),
                        "PRIMARY_NUMBER": person.get("primaryNumber"),
                        "BIRTH_DATE": person.get("birthDate"),
                        "CURRENT_AGE": person.get("currentAge"),
                        "BIRTH_CITY": person.get("birthCity"),
                        "BIRTH_STATE_PROVINCE": person.get("birthStateProvince"),
                        "BIRTH_COUNTRY": person.get("birthCountry"),
                        "HEIGHT": person.get("height"),
                        "WEIGHT": person.get("weight"),
                        "ACTIVE": person.get("active", False),
                        
                        # Player primary position
                        "PRIMARY_POSITION_CODE": person.get("primaryPosition", {}).get("code"),
                        "PRIMARY_POSITION_NAME": person.get("primaryPosition", {}).get("name"),
                        "PRIMARY_POSITION_TYPE": person.get("primaryPosition", {}).get("type"),
                        "PRIMARY_POSITION_ABBREVIATION": person.get("primaryPosition", {}).get("abbreviation"),
                        
                        # Player characteristics
                        "BATS": person.get("batSide", {}).get("code"),
                        "THROWS": person.get("pitchHand", {}).get("code"),
                        "DEBUT_DATE": person.get("mlbDebutDate"),
                        "SERVICE_TIME": person.get("serviceTime"),
                        "ROOKIE_STATUS": person.get("rookieStatus", False),
                        
                        # Hitting statistics
                        "HITTING_AVG": hitting_stats.get("avg"),
                        "HITTING_OBP": hitting_stats.get("obp"),
                        "HITTING_SLG": hitting_stats.get("slg"),
                        "HITTING_OPS": hitting_stats.get("ops"),
                        "HITTING_GAMES": hitting_stats.get("gamesPlayed"),
                        "HITTING_AB": hitting_stats.get("atBats"),
                        "HITTING_RUNS": hitting_stats.get("runs"),
                        "HITTING_HITS": hitting_stats.get("hits"),
                        "HITTING_DOUBLES": hitting_stats.get("doubles"),
                        "HITTING_TRIPLES": hitting_stats.get("triples"),
                        "HITTING_HR": hitting_stats.get("homeRuns"),
                        "HITTING_RBI": hitting_stats.get("rbi"),
                        "HITTING_BB": hitting_stats.get("baseOnBalls"),
                        "HITTING_SO": hitting_stats.get("strikeOuts"),
                        "HITTING_SB": hitting_stats.get("stolenBases"),
                        "HITTING_CS": hitting_stats.get("caughtStealing"),
                        "HITTING_HBP": hitting_stats.get("hitByPitch"),
                        "HITTING_IBB": hitting_stats.get("intentionalWalks"),
                        
                        # Pitching statistics
                        "PITCHING_WINS": pitching_stats.get("wins"),
                        "PITCHING_LOSSES": pitching_stats.get("losses"),
                        "PITCHING_ERA": pitching_stats.get("era"),
                        "PITCHING_GAMES": pitching_stats.get("gamesPlayed"),
                        "PITCHING_GS": pitching_stats.get("gamesStarted"),
                        "PITCHING_CG": pitching_stats.get("completeGames"),
                        "PITCHING_SHO": pitching_stats.get("shutouts"),
                        "PITCHING_SV": pitching_stats.get("saves"),
                        "PITCHING_HOLDS": pitching_stats.get("holds"),
                        "PITCHING_IP": pitching_stats.get("inningsPitched"),
                        "PITCHING_HITS": pitching_stats.get("hits"),
                        "PITCHING_RUNS": pitching_stats.get("runs"),
                        "PITCHING_ER": pitching_stats.get("earnedRuns"),
                        "PITCHING_HR": pitching_stats.get("homeRuns"),
                        "PITCHING_BB": pitching_stats.get("baseOnBalls"),
                        "PITCHING_SO": pitching_stats.get("strikeOuts"),
                        "PITCHING_WHIP": pitching_stats.get("whip"),
                        "PITCHING_AVG": pitching_stats.get("avg"),
                        "PITCHING_K9": pitching_stats.get("strikeoutsPer9Inn"),
                        
                        # Fielding statistics
                        "FIELDING_POSITION": fielding_stats.get("position", {}).get("code"),
                        "FIELDING_GAMES": fielding_stats.get("games"),
                        "FIELDING_GS": fielding_stats.get("gamesStarted"),
                        "FIELDING_INNINGS": fielding_stats.get("innings"),
                        "FIELDING_CHANCES": fielding_stats.get("chances"),
                        "FIELDING_PUTOUTS": fielding_stats.get("putOuts"),
                        "FIELDING_ASSISTS": fielding_stats.get("assists"),
                        "FIELDING_ERRORS": fielding_stats.get("errors"),
                        "FIELDING_DOUBLEPLAYS": fielding_stats.get("doublePlays"),
                        "FIELDING_FIELDINGPERCENTAGE": fielding_stats.get("fielding"),
                        
                        # Metadata
                        "LOADED_TIMESTAMP": datetime.datetime.now(),
                        
                        # Additional data as JSON
                        "ADDITIONAL_DATA": json.dumps({"source": "MLB Stats API"})
                    }
                    
                    team_players.append(player_info)
                    year_players.append(player_info)
                    all_players.append(player_info)
                    time.sleep(0.25)  # Small delay to avoid rate limiting
                    
                except Exception as e:
                    print(f"Error processing player {player_id}: {e}")
                    continue
            
            print(f"Processed {len(team_players)} players for {team_name} in {year}")
            total_players_processed += len(team_players)
            
            # Load data for this team immediately to avoid memory issues with large datasets
            if team_players:
                try:
                    df_team = pd.DataFrame(team_players, columns=columns)
                    
                    # Insert data for this team
                    print(f"Inserting {len(team_players)} players for {team_name} ({year}) into MotherDuck...")
                    conn.execute("INSERT INTO mlb_data.mlb_team_rosters SELECT * FROM df_team")
                    
                    # Verify data
                    count = conn.execute("SELECT COUNT(*) FROM mlb_data.mlb_team_rosters WHERE TEAM_ID = ? AND SEASON = ?", 
                                        [team_id, year]).fetchone()[0]
                    print(f"Now have {count} {team_name} players for {year} in the database")
                except Exception as e:
                    print(f"Error inserting data for {team_name} in {year}: {e}")
            
            year_teams_processed += 1
            total_teams_processed += 1
            
            # Sleep between teams to avoid overloading the API
            if not (team == mlb_teams[-1] and year == years[-1]):  # Don't sleep after the last team and year
                print(f"Waiting 2 seconds before processing next team...")
                time.sleep(2)
                
        except Exception as e:
            print(f"Error processing team {team_name} in {year}: {e}")
            continue
    
    print(f"\n===== Year {year} Summary =====")
    print(f"Teams processed: {year_teams_processed}")
    print(f"Players processed: {len(year_players)}")
    
    # Sleep between years to avoid overloading the API
    if year != years[-1]:  # Don't sleep after the last year
        print(f"Waiting 5 seconds before processing next year...")
        time.sleep(5)

# Final verification
print("\n===== Final Statistics =====")
total_count = conn.execute("SELECT COUNT(*) FROM mlb_data.mlb_team_rosters").fetchone()[0]
print(f"Total teams processed: {total_teams_processed}")
print(f"Total players processed in this run: {total_players_processed}")
print(f"Total records in database: {total_count}")

# Get counts by year
print("\n===== Records by Year =====")
year_counts = conn.execute("SELECT SEASON, COUNT(*) FROM mlb_data.mlb_team_rosters GROUP BY SEASON ORDER BY SEASON").fetchall()
for year, count in year_counts:
    print(f"Year {year}: {count} players")

# Get counts by team for the latest year
latest_year = max(years)
print(f"\n===== Team Counts for {latest_year} =====")
team_counts = conn.execute("""
    SELECT TEAM_NAME, COUNT(*) as player_count 
    FROM mlb_data.mlb_team_rosters 
    WHERE SEASON = ? 
    GROUP BY TEAM_NAME 
    ORDER BY player_count DESC
""", [latest_year]).fetchall()
for team, count in team_counts:
    print(f"{team}: {count} players")

# Close connection
conn.close()
print("\nDone! All MLB teams data has been successfully loaded into MotherDuck.")
