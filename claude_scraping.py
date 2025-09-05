import requests
import pandas as pd
import time
from typing import Dict, List, Optional
import json

class RobustScraper:
    def __init__(self):
        self.base_url = "https://fbrapi.com"
        self.api_key = None
        self.premier_league_id = 9

    def generate_api_key(self) -> str:
        try:
            response = requests.post(f"{self.base_url}/generate_api_key")
            response.raise_for_status()
            api_key = response.json()['api_key']
            self.api_key = api_key
            print(f"API Key generated successfully: {api_key}")
            return api_key
        except requests.exceptions.RequestException as e:
            print(f"Error generating API key: {e}")
            return None
    
    def make_request(self, endpoint: str, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        if not self.api_key:
            print("No API key available. Please generate one first")
            return None
        
        headers = {"X-API-Key": self.api_key}
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(max_retries):
            try:
                print(f"Making request to {endpoint} (attempt {attempt + 1}/{max_retries})")
                time.sleep(6)
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code == 500:
                    print(f"Server error (500) on attempt {attempt + 1}. Retrying...")
                    if attempt < max_retries - 1:
                        time.sleep(10)
                        continue
                    else:
                        print(f"Server error persists after {max_retries} attempts for {endpoint}")
                        return None
                
                response.raise_for_status()
                data = response.json()
                print(f"Successfully got data from {endpoint}")
                return data
                
            except requests.exceptions.RequestException as e:
                print(f"Error on attempt {attempt + 1} for {endpoint}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                    continue
                else:
                    print(f"All attempts failed for {endpoint}")
                    return None
        
        return None
        
    def get_available_seasons(self) -> List[str]:
        data = self.make_request("league-seasons", {"league_id": self.premier_league_id})
        if data and 'data' in data:
            return [season['season_id'] for season in data['data']]
        return []
    
    def get_teams_via_matches(self, season_id: str) -> List[Dict]:
        """Alternative method to get teams by looking at match data"""
        print(f"Trying alternative method to get teams for {season_id}")
        
        data = self.make_request("matches", {
            "league_id": self.premier_league_id,
            "season_id": season_id
        })
        
        teams = {}
        if data and 'data' in data:
            for match in data['data']:
                if 'home' in match and 'home_team_id' in match:
                    teams[match['home_team_id']] = match['home']
                if 'away' in match and 'away_team_id' in match:
                    teams[match['away_team_id']] = match['away']
        
        team_list = [{'team_id': team_id, 'team_name': team_name} 
                    for team_id, team_name in teams.items()]
        
        print(f"Found {len(team_list)} teams via matches method")
        return team_list
    
    def get_teams_for_season(self, season_id: str) -> List[Dict]:
        """Try league-standings first, fallback to matches method"""
        print(f"Getting teams for season {season_id}")
        
        data = self.make_request("league-standings", {
            "league_id": self.premier_league_id,
            "season_id": season_id
        }, max_retries=2)

        teams = []
        if data and 'data' in data:
            for standings_group in data['data']:
                if 'standings' in standings_group:
                    for team in standings_group['standings']:
                        teams.append({
                            'team_id': team['team_id'],
                            'team_name': team['team_name']
                        })

        if not teams:
            print("League-standings failed, trying alternative method...")
            teams = self.get_teams_via_matches(season_id)
        
        print(f"Total teams found: {len(teams)}")
        return teams
    
    def get_team_matches(self, team_id: str, season_id: str) -> List[Dict]:
        print(f"Getting matches for team {team_id} in {season_id}")
        data = self.make_request("matches", {
            "team_id": team_id,
            "league_id": self.premier_league_id,
            "season_id": season_id
        })

        if data and 'data' in data:
            print(f"✅ Found {len(data['data'])} matches")
            return data['data']
        return []
    
    def get_team_match_stats(self, team_id: str, season_id: str) -> List[Dict]:
        print(f"Getting match stats for team {team_id} in {season_id}")
        data = self.make_request("team-match-stats", {
            "team_id": team_id,
            "league_id": self.premier_league_id,
            "season_id": season_id,
        })

        if data and 'data' in data:
            print(f"Found stats for {len(data['data'])} matches")
            return data['data']
        return []
    
    def process_team_data(self, team_id: str, team_name: str, season_id: str) -> pd.DataFrame:
        print(f"\nProcessing {team_name} for {season_id}...")

        matches = self.get_team_matches(team_id, season_id)
        if not matches:
            print(f"No matches found for {team_name} in {season_id}")
            return pd.DataFrame()
        
        match_stats = self.get_team_match_stats(team_id, season_id)
        if not match_stats:
            print(f"No match stats found for {team_name} in {season_id}")

        matches_data = []
        for match in matches:
            matches_data.append({
                'Date': match['date'],
                'Time': match.get('time', ''),
                'Comp': 'Premier League',
                'Round': match.get('round', ''),
                'Venue': 'Home' if match.get('home_away') == 'Home' else 'Away',
                'Result': match.get('result', ''),
                'GF': match.get('gf', 0),
                'GA': match.get('ga', 0),
                'Opponent': match.get('opponent', ''),
                'Formation': match.get('formation', ''),
                'Referee': match.get('referee', ''),
                'Match Report': '',
                'Notes': ''
            })

        matches_df = pd.DataFrame(matches_data)
        
        shooting_data = []
        if match_stats:
            for match_stat in match_stats:
                if 'stats' in match_stat and 'shooting' in match_stat['stats']:
                    shooting_stats = match_stat['stats']['shooting']
                    shooting_data.append({
                        'Date': match_stat['meta_data']['date'],
                        'Sh': shooting_stats.get('sh', 0),
                        'SoT': shooting_stats.get('sot', 0),
                        'Dist': shooting_stats.get('avg_sh_dist', 0),
                        'FK': shooting_stats.get('fk_sh', 0),
                        'PK': shooting_stats.get('pk_made', 0),
                        'PKatt': match_stat['stats'].get('schedule', {}).get('pk_att', 0) if 'schedule' in match_stat['stats'] else 0
                    })

        try:
            if shooting_data:
                shooting_df = pd.DataFrame(shooting_data)
                team_data = matches_df.merge(
                    shooting_df[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], 
                    on="Date", 
                    how="left"
                )
                print(f"Merged shooting stats for {len(shooting_df)} matches")
            else:
                team_data = matches_df.copy()
                for col in ["Sh", "SoT", "Dist", "FK", "PK", "PKatt"]:
                    team_data[col] = 0
                print("No shooting stats available, added empty columns")
        except Exception as e:
            print(f"Merge error for {team_name}: {e}")
            return pd.DataFrame()
        
        team_data = team_data[team_data["Comp"] == "Premier League"]

        team_data["Season"] = season_id
        team_data["Team"] = team_name
        
        print(f"Processed {len(team_data)} matches for {team_name}")
        return team_data
    
    def scrape_premier_league_data(self, seasons: List[str]) -> pd.DataFrame:
        if not self.api_key:
            print("Generating API key...")
            if not self.generate_api_key():
                print("Failed to generate API key")
                return pd.DataFrame()
            
        all_matches = []

        for season_id in seasons:
            print(f"\nProcessing season {season_id}...")

            teams = self.get_teams_for_season(season_id)
            if not teams:
                print(f"No teams found for season {season_id}")
                continue

            print(f"Found {len(teams)} teams for {season_id}")

            for i, team in enumerate(teams, 1):
                try:
                    print(f"\nProcessing team {i}/{len(teams)}: {team['team_name']}")
                    
                    team_data = self.process_team_data(
                        team['team_id'],
                        team['team_name'],
                        season_id
                    )

                    if not team_data.empty:
                        all_matches.append(team_data)
                        print(f"Added {len(team_data)} matches for {team['team_name']}")
                    else:
                        print(f"No data collected for {team['team_name']}")

                    print("Waiting 3 seconds...")
                    time.sleep(6)

                except Exception as e:
                    print(f"Error processing {team['team_name']}: {e}")
                    continue
        
        if all_matches:
            print(f"\nCombining data from {len(all_matches)} team-seasons...")
            match_df = pd.concat(all_matches, ignore_index=True)
            match_df.columns = [c.lower() for c in match_df.columns]
            print(f"Final dataset shape: {match_df.shape}")
            return match_df
        else:
            print("No data collected")
            return pd.DataFrame()

def main():

    scraper = RobustScraper()

    seasons = ["2023-2024", "2024-2025"]

    print(f"Starting to scrape Premier League data for seasons: {seasons}")

    match_df = scraper.scrape_premier_league_data(seasons)

    if not match_df.empty:
        print(f"\nSUCCESS! Collected {len(match_df)} matches")
        print(f"Data shape: {match_df.shape}")
        print(f"Seasons covered: {sorted(match_df['season'].unique())}")
        print(f"Teams covered: {len(match_df['team'].unique())}")
        
        print("\nColumn names:")
        print(list(match_df.columns))
        
        print(f"\nFirst few rows:")
        print(match_df.head())
        
        print(f"\nShooting stats summary:")
        shooting_cols = ['sh', 'sot', 'dist', 'fk', 'pk', 'pkatt']
        available_shooting_cols = [col for col in shooting_cols if col in match_df.columns]
        if available_shooting_cols:
            print(match_df[available_shooting_cols].describe())
        
        filename = "matches_2023_2025.csv"
        match_df.to_csv(filename, index=False)
        print(f"\nData saved to {filename}")
        print("Ready for machine learning analysis!")
        
    else:
        print("No data was collected.")
        print("\nTroubleshooting suggestions:")
        print("1. The FBR API might be experiencing server issues")
        print("2. Try running the script later")
        print("3. Check if the seasons exist in the available_seasons list")

def test_single_season():
    """Test with just one season to debug issues"""
    scraper = RobustScraper()
    
    seasons = ["2023-2024"]
    
    print(f"Testing with single season: {seasons}")
    match_df = scraper.scrape_premier_league_data(seasons)
    
    if not match_df.empty:
        print(f"Test successful! Got {len(match_df)} matches")
        match_df.to_csv("test_matches.csv", index=False)
    else:
        print("Test failed")

if __name__ == "__main__":
    main()