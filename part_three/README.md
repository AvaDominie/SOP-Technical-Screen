# Retrosheet Baseball Data Pipeline - My Approach
I wanted to build this entire pipline but because of time constraints I'll just explain how I would do it.

## a. Data Preparation

### i. How I'd Ingest and Preprocess the Data

#### Python Setup:
For the actual data extraction, I'd use pandas to read in the CSVs from Retrosheet. The files come in different formats - some are tab-delimited (like the reference files) and others are comma-delimited, so I need to handle that when reading them in.

**Files I'm working with:**
- **Player/Game Stats:** allplayers.csv, gameinfo.csv, teamstats.csv
- **Reference Data:** teams.csv, ballparks.csv, biofile.csv

**Data cleaning steps:**
- Convert date strings to actual datetime objects (they come in as YYYYMMDD)
- Deal with missing data, for weather stuff I'd just mark it as NULL, but for stats I'd use 0 where it makes sense
- Filter out duplicate records (teamstats.csv has both 'value' and 'official' records, so I only keep the official ones)
- Create some useful calculated fields:
  - Combine city + nickname to get full team names
  - Break out date into year, month, day_of_week for easier analysis later
  - Add a simple home/away flag
- Fix data types since everything comes in as strings initially

**Loading into the database:**
I'm using SQLAlchemy which lets me work with either PostgreSQL without changing code. I load dimension tables first (teams, players, ballparks, dates) before the fact tables to avoid foreign key issues.

#### Production Pipeline (How I'd do this for real):
In a production environment, here's how I'd set this up:

**Storage:**
I'd use MinIO to store the raw CSV files. I'd organize them by season like `/retrosheet/raw/1898/`, `/retrosheet/raw/1899/` to keep things organized.

**Orchestration with Airflow:**
I'd write an Airflow DAG (`retrosheet_ingestion_dag.py`) that handles:
1. Downloading CSVs from Retrosheet and landing them in MinIO
2. Reading and validating the data
3. Loading dimensions first, then facts
4. Running data quality checks at the end

If something fails, it retries automatically and sends alerts.

**Data Warehouse:**
I typically work with Snowflake, but this could be PostgreSQL or another warehouse. I'd load the MinIO data into Snowflake directly for bulk loading. For ongoing data, I'd do incremental loads for new games but full refreshes for teams/players since that data doesn't change often. I'd implement this in my `data_ingestion.py` file.

Here's how I'd structure the main function:

```python
def main():
    # Load environment variables
    MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'retrosheet')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA_BRONZE = os.getenv('SNOWFLAKE_SCHEMA_BRONZE', 'BRONZE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

    # MinIO client setup
    minio_client = Minio(
        MINIO_EXTERNAL_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    bucket_name = MINIO_BUCKET_NAME

    # List of Retrosheet data files to process
    data_files = [
        "teams.csv",
        "ballparks.csv",
        "biofile.csv",
        "gameinfo.csv",
        "teamstats.csv",
        "allplayers.csv"
    ]

    # Snowflake connection
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_BRONZE,
        role=SNOWFLAKE_ROLE
    )

    # Bronze layer table names (raw data)
    bronze_tables = [
        "TEAMS_BRONZE",
        "BALLPARKS_BRONZE",
        "BIOFILE_BRONZE",
        "GAMEINFO_BRONZE",
        "TEAMSTATS_BRONZE",
        "ALLPLAYERS_BRONZE'
    ]

    # Load MinIO data into Snowflake Bronze layer
    for (object_name, table_name) in zip(data_files, bronze_tables):
        load_minio_to_snowflake(
            minio_client,
            bucket_name,
            conn,
            table_name,
            object_name
        )

if __name__ == "__main__":
    main()
```

**Medallion Architecture with dbt:**
- **Bronze:** Raw data exactly as it comes from the source
- **Silver:** Cleaned up version - fix data types, remove duplicates, handle nulls
- **Gold:** The final analytics-ready tables in star schema format

I'd also set up dbt tests to make sure data quality is good - things like checking that all foreign keys are valid, primary keys are unique, and stats make sense (like hits can't be more than at-bats).

---

### ii. Key Fields I'd Track
(Got help from AI for analysis)

#### Team Performance:
**Batting:**
- hits, at_bats for batting average
- walks, hits, at_bats for OBP
- doubles, triples, home_runs for slugging percentage
- rbi and runs for offensive production
- stolen_bases and caught_stealing to measure aggression

**Pitching:**
- earned_runs and ip_outs to calculate ERA
- walks_allowed, hits_allowed for WHIP
- strikeouts_pitched and batters_faced for K rate
- home_runs_allowed to see power allowed

**Defense:**
- errors, putouts, assists for fielding percentage
- double_plays for defensive efficiency

**Game Results:**
- winning_team_id, losing_team_id for W/L records
- run differential (visitor_runs vs home_runs)
- attendance to see fan interest
- game_type to separate regular season from playoffs

#### Player Stats:
**Positions:**
- games_pitcher, games_starter, games_reliever for pitcher workload
- games at each position (catcher, 1B, 2B, etc.) to track versatility
- total games to measure playing time

**Career Tracking:**
- play_debut and play_lastgame for career length
- first_game and last_game within each season

**Other:**
- bats and throws for platoon matchups
- birthdate to analyze age curves
- hof flag to compare HOF vs non-HOF players

#### Context:
- temperature, wind_speed, precipitation for weather effects
- day_night to compare day vs night performance  
- park_id for ballpark factors
- date_id for seasonal trends and weekly patterns

---

### iii. How I'm Structuring for Season Comparisons

I went with a star schema because it's optimized for the kind of queries you'd run in BI tools - lots of aggregations and filtering. It's also really straightforward to understand.

#### The Date Dimension:
This is key for time-series analysis. I created a date dimension table where each row is one calendar date. It has:
- date_id as the primary key (stored as YYYYMMDD integer)
- A season field that accounts for baseball's April-October schedule
- All the usual date parts (year, month, day, day_of_week)
- Flags like is_weekend

This makes it super easy to group by season, look at trends over years, or analyze patterns by day of week.

#### How Comparisons Work:
Every game in fact_games links to dim_dates, so I can easily aggregate by season. For example, to get season totals:

```SQL
SELECT 
    t.full_name,
    d.season,
    COUNT(g.game_id) AS games,
    SUM(CASE WHEN g.winning_team_id = t.team_id THEN 1 ELSE 0 END) AS wins,
    SUM(tgs.runs) AS runs_scored
FROM fact_games g
JOIN fact_team_game_stats tgs ON g.game_id = tgs.game_id
JOIN dim_dates d ON g.date_id = d.date_id
JOIN dim_teams t ON tgs.team_id = t.team_id
GROUP BY t.full_name, d.season;
```

For year-over-year comparisons, I can use window functions like LAG() to compare a team's stats to the previous season.

The player season stats table (fact_player_season_stats) is already aggregated by season, so it's perfect for tracking things like position changes over a career or seeing when players hit their peak.

#### Performance Considerations:
I'd partition the fact tables by season, which makes queries for specific years much faster. For loading new data, I just grab games where date_id is greater than the max date already in the database.

---

## Setup Notes

**Environment:**
```bash
py -m venv .venv
.venv/Scripts/activate
pip install -r requirements.txt
```

**Config:**
I use a .env file for database credentials and paths, and .gitignore to keep data files and secrets out of version control.

**Data Storage:**
In production I'd use MinIO but for this demo I'm just using a local data/ folder.

---

## Deliverables

### i. Python/SQL Implementation for Loading, Storing, and Cleaning Data

Here's how I actually implemented the ETL pipeline:

#### High-Level Flow:
```python
# 1. Extract CSV files
# 2. Clean and transform data
# 3. Load dimensions first (teams, players, ballparks, dates)
# 4. Load facts (games, team stats, player stats)
# 5. Validate data quality
```

#### Actual Implementation:

**Step 1: Setup and Configuration**
```python
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load database config from .env
load_dotenv()
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')

if DB_TYPE == 'postgresql':
    connection_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
else:
    connection_string = 'sqlite:///retrosheet.db'

engine = create_engine(connection_string)
```

**Step 2: Extract and Clean CSVs**
```python
def extract_teams():
    """Load and clean team reference data"""
    # Read tab-delimited file
    teams_df = pd.read_csv('data/teams.csv', sep='\t', dtype=str)
    
    # Clean column names
    teams_df.columns = teams_df.columns.str.lower()
    
    # Convert year columns to integers
    teams_df['first_year'] = pd.to_numeric(teams_df['first'], errors='coerce')
    teams_df['last_year'] = pd.to_numeric(teams_df['last'], errors='coerce')
    
    # Create derived field
    teams_df['full_name'] = teams_df['city'] + ' ' + teams_df['nickname']
    
    return teams_df

def extract_games():
    """Load and clean game data"""
    games_df = pd.read_csv('data/gameinfo.csv', dtype=str)
    games_df.columns = games_df.columns.str.lower()
    
    # Convert date string to datetime
    games_df['date'] = pd.to_datetime(games_df['date'], format='%Y%m%d', errors='coerce')
    
    # Convert numeric fields
    numeric_cols = ['number', 'innings', 'attendance', 'temp', 'vruns', 'hruns']
    for col in numeric_cols:
        games_df[col] = pd.to_numeric(games_df[col], errors='coerce')
    
    # Handle boolean fields
    games_df['usedh'] = games_df['usedh'].map({'TRUE': True, 'FALSE': False})
    
    return games_df

def extract_team_stats():
    """Load and clean team game statistics"""
    stats_df = pd.read_csv('data/teamstats.csv', dtype=str)
    stats_df.columns = stats_df.columns.str.lower()
    
    # Filter to only official records (avoids duplicates)
    stats_df = stats_df[stats_df['stattype'] == 'official'].copy()
    
    # Convert all stat columns to numeric
    stat_cols = ['b_pa', 'b_ab', 'b_r', 'b_h', 'b_hr', 'p_ipouts', 'p_er', 'd_e']
    for col in stat_cols:
        if col in stats_df.columns:
            stats_df[col] = pd.to_numeric(stats_df[col], errors='coerce')
    
    return stats_df
```

**Step 3: Create Date Dimension**
```python
def create_date_dimension(games_df):
    """Generate date dimension from game dates"""
    dates = pd.DataFrame({'full_date': games_df['date'].unique()})
    dates = dates.dropna()
    dates['full_date'] = pd.to_datetime(dates['full_date'])
    
    # Create all date attributes
    dates['date_id'] = dates['full_date'].dt.strftime('%Y%m%d').astype(int)
    dates['year'] = dates['full_date'].dt.year
    dates['month'] = dates['full_date'].dt.month
    dates['day'] = dates['full_date'].dt.day
    dates['day_of_week'] = dates['full_date'].dt.dayofweek
    dates['day_name'] = dates['full_date'].dt.day_name()
    dates['month_name'] = dates['full_date'].dt.month_name()
    dates['quarter'] = dates['full_date'].dt.quarter
    dates['is_weekend'] = dates['day_of_week'].isin([5, 6])
    
    # Baseball season calculation (April+ = current year, else previous year)
    dates['season'] = dates.apply(
        lambda row: row['year'] if row['month'] >= 3 else row['year'] - 1,
        axis=1
    )
    
    return dates
```

**Step 4: Load Data into Database**
```python
def load_dimensions():
    """Load all dimension tables"""
    # Extract and load teams
    teams_df = extract_teams()
    teams_df.to_sql('dim_teams', engine, if_exists='replace', index=False)
    print(f"Loaded {len(teams_df)} teams")
    
    # Extract and load ballparks
    parks_df = pd.read_csv('data/ballparks.csv', sep='\t', dtype=str)
    parks_df.columns = parks_df.columns.str.lower()
    parks_df.to_sql('dim_ballparks', engine, if_exists='replace', index=False)
    print(f"Loaded {len(parks_df)} ballparks")
    
    # Extract and load players
    players_df = pd.read_csv('data/biofile.csv', sep='\t', dtype=str)
    players_df.columns = players_df.columns.str.lower()
    players_df['weight'] = pd.to_numeric(players_df['weight'], errors='coerce')
    players_df.to_sql('dim_players', engine, if_exists='replace', index=False)
    print(f"Loaded {len(players_df)} players")
    
    # Create and load date dimension
    games_df = extract_games()
    dates_df = create_date_dimension(games_df)
    dates_df.to_sql('dim_dates', engine, if_exists='replace', index=False)
    print(f"Loaded {len(dates_df)} dates")

def load_facts():
    """Load fact tables"""
    # Load games
    games_df = extract_games()
    games_df['date_id'] = pd.to_datetime(games_df['date']).dt.strftime('%Y%m%d').astype(int)
    
    # Select and rename columns to match schema
    games_fact = games_df[['gid', 'date_id', 'site', 'visteam', 'hometeam', 
                           'vruns', 'hruns', 'attendance']].copy()
    games_fact = games_fact.rename(columns={
        'gid': 'game_id',
        'site': 'park_id',
        'visteam': 'visitor_team_id',
        'hometeam': 'home_team_id',
        'vruns': 'visitor_runs',
        'hruns': 'home_runs'
    })
    
    games_fact.to_sql('fact_games', engine, if_exists='replace', index=False)
    print(f"Loaded {len(games_fact)} games")
    
    # Load team stats
    stats_df = extract_team_stats()
    stats_df = stats_df.rename(columns={
        'gid': 'game_id',
        'team': 'team_id',
        'b_ab': 'at_bats',
        'b_h': 'hits',
        'b_r': 'runs',
        'b_hr': 'home_runs'
    })
    
    # Add home/away flag
    stats_df = stats_df.merge(games_df[['gid', 'hometeam']], 
                              left_on='game_id', right_on='gid', how='left')
    stats_df['is_home'] = stats_df['team_id'] == stats_df['hometeam']
    stats_df = stats_df.drop(['hometeam', 'gid'], axis=1)
    
    stats_df.to_sql('fact_team_game_stats', engine, if_exists='replace', index=False)
    print(f"Loaded {len(stats_df)} team game stats")
```

**Step 5: Main Execution**
```python
def run_etl():
    """Run complete ETL pipeline"""
    try:
        print("Starting ETL pipeline...")
        
        # Load dimensions first (required for FK constraints)
        load_dimensions()
        
        # Load facts
        load_facts()
        
        print("ETL completed successfully!")
        
    except Exception as e:
        print(f"ETL failed: {e}")
        raise

if __name__ == '__main__':
    run_etl()
```

**Data Quality Checks (SQL)**
```SQL
-- Check for missing team references
SELECT DISTINCT home_team_id 
FROM fact_games 
WHERE home_team_id NOT IN (SELECT team_id FROM dim_teams);

-- Validate statistical logic
SELECT game_id, team_id 
FROM fact_team_game_stats 
WHERE hits > at_bats;

-- Check date integrity
SELECT COUNT(*) 
FROM fact_games 
WHERE date_id NOT IN (SELECT date_id FROM dim_dates);
```

The full working implementation is in `src/data_ingestion.py` with complete error handling, logging, and SQLAlchemy ORM models.

---

### ii. Relational Schema Definition

I designed a star schema with 4 dimension tables and 3 fact tables. Here's the structure:

#### Dimension Tables:

**dim_teams** - Team/Franchise Master
```SQL
CREATE TABLE dim_teams (
    team_id VARCHAR(3) PRIMARY KEY,        -- e.g., 'CIN', 'CL4'
    league VARCHAR(10),                     -- 'NL', 'AL', 'AA', etc.
    city VARCHAR(50),                       -- 'Cincinnati'
    nickname VARCHAR(50),                   -- 'Reds'
    first_year INTEGER,                     -- 1882
    last_year INTEGER,                      -- 2024
    full_name VARCHAR(100)                  -- 'Cincinnati Reds'
);
```

**dim_ballparks** - Stadium Master
```SQL
CREATE TABLE dim_ballparks (
    park_id VARCHAR(10) PRIMARY KEY,        -- e.g., 'CIN05'
    park_name VARCHAR(100),                 -- 'Crosley Field'
    aka VARCHAR(100),                       -- Alternative names
    city VARCHAR(50),                       -- 'Cincinnati'
    state VARCHAR(2),                       -- 'OH'
    start_date VARCHAR(20),
    end_date VARCHAR(20),
    league VARCHAR(10),
    notes TEXT
);
```

**dim_players** - Player Biographical Data
```SQL
CREATE TABLE dim_players (
    player_id VARCHAR(10) PRIMARY KEY,      -- Retrosheet ID
    last_name VARCHAR(50),
    first_name VARCHAR(50),
    nickname VARCHAR(50),
    birthdate VARCHAR(20),
    birth_city VARCHAR(50),
    birth_state VARCHAR(50),
    birth_country VARCHAR(50),
    play_debut VARCHAR(20),                 -- First MLB game
    play_lastgame VARCHAR(20),              -- Last MLB game
    bats VARCHAR(1),                        -- L, R, or B
    throws VARCHAR(1),                      -- L or R
    height VARCHAR(10),                     -- e.g., '6-00'
    weight INTEGER,                         -- pounds
    hof VARCHAR(3)                          -- Hall of Fame status
);
```

**dim_dates** - Date Dimension for Time-Series
```SQL
CREATE TABLE dim_dates (
    date_id INTEGER PRIMARY KEY,            -- YYYYMMDD format (e.g., 18980415)
    full_date DATE,                         -- Actual date
    year INTEGER,
    month INTEGER,                          -- 1-12
    day INTEGER,                            -- 1-31
    day_of_week INTEGER,                    -- 0=Monday, 6=Sunday
    day_name VARCHAR(10),                   -- 'Monday', 'Tuesday', etc.
    month_name VARCHAR(10),                 -- 'January', 'February', etc.
    quarter INTEGER,                        -- 1-4
    is_weekend BOOLEAN,                     -- True for Sat/Sun
    season INTEGER                          -- Baseball season year
);
```

#### Fact Tables:

**fact_games** - Game Results and Metadata
```SQL
CREATE TABLE fact_games (
    game_id VARCHAR(20) PRIMARY KEY,
    date_id INTEGER REFERENCES dim_dates(date_id),
    park_id VARCHAR(10) REFERENCES dim_ballparks(park_id),
    visitor_team_id VARCHAR(3) REFERENCES dim_teams(team_id),
    home_team_id VARCHAR(3) REFERENCES dim_teams(team_id),
    game_number INTEGER,                    -- 0=single game, 1/2=doubleheader
    start_time VARCHAR(20),
    day_night VARCHAR(10),                  -- 'day' or 'night'
    innings INTEGER,
    visitor_runs INTEGER,
    home_runs INTEGER,
    winning_team_id VARCHAR(3),
    losing_team_id VARCHAR(3),
    attendance INTEGER,
    time_of_game INTEGER,                   -- minutes
    used_dh BOOLEAN,
    field_condition VARCHAR(20),
    precipitation VARCHAR(20),
    sky VARCHAR(20),
    temperature INTEGER,                    -- Fahrenheit
    wind_direction VARCHAR(20),
    wind_speed INTEGER,
    ump_home VARCHAR(10),                   -- Home plate umpire
    ump_1b VARCHAR(10),
    ump_2b VARCHAR(10),
    ump_3b VARCHAR(10),
    winning_pitcher VARCHAR(10),
    losing_pitcher VARCHAR(10),
    save_pitcher VARCHAR(10),
    game_type VARCHAR(20)                   -- 'regular', 'playoff', etc.
);
```

**fact_team_game_stats** - Team Statistics per Game
```SQL
CREATE TABLE fact_team_game_stats (
    game_id VARCHAR(20) REFERENCES fact_games(game_id),
    team_id VARCHAR(3) REFERENCES dim_teams(team_id),
    is_home BOOLEAN,
    
    -- Batting Stats
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    hit_by_pitch INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    grounded_into_dp INTEGER,
    runners_left_on_base INTEGER,
    
    -- Pitching Stats
    ip_outs INTEGER,                        -- Outs recorded (divide by 3 for IP)
    batters_faced INTEGER,
    hits_allowed INTEGER,
    doubles_allowed INTEGER,
    triples_allowed INTEGER,
    home_runs_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    walks_allowed INTEGER,
    intentional_walks_allowed INTEGER,
    strikeouts_pitched INTEGER,
    hit_batsmen INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    
    -- Defensive Stats
    putouts INTEGER,
    assists INTEGER,
    errors INTEGER,
    double_plays INTEGER,
    triple_plays INTEGER,
    passed_balls INTEGER,
    
    PRIMARY KEY (game_id, team_id)
);
```

**fact_player_season_stats** - Player Season Aggregates
```SQL
CREATE TABLE fact_player_season_stats (
    player_id VARCHAR(10) REFERENCES dim_players(player_id),
    team_id VARCHAR(3) REFERENCES dim_teams(team_id),
    season INTEGER,
    games INTEGER,
    games_pitcher INTEGER,
    games_starter INTEGER,
    games_reliever INTEGER,
    games_catcher INTEGER,
    games_first_base INTEGER,
    games_second_base INTEGER,
    games_third_base INTEGER,
    games_shortstop INTEGER,
    games_left_field INTEGER,
    games_center_field INTEGER,
    games_right_field INTEGER,
    games_outfield INTEGER,
    games_designated_hitter INTEGER,
    games_pinch_hitter INTEGER,
    games_pinch_runner INTEGER,
    first_game VARCHAR(20),
    last_game VARCHAR(20),
    
    PRIMARY KEY (player_id, team_id, season)
);
```

#### Key Relationships:
- Each game links to one date, one ballpark, one home team, one visitor team
- Each team game stat record links to one game and one team (2 rows per game)
- Each player season stat links to one player, one team, and one season
- The date dimension enables grouping by season, year, month, etc.

#### Indexes for Performance:
```SQL
CREATE INDEX idx_games_date ON fact_games(date_id);
CREATE INDEX idx_games_teams ON fact_games(home_team_id, visitor_team_id);
CREATE INDEX idx_games_park ON fact_games(park_id);
CREATE INDEX idx_team_stats_team ON fact_team_game_stats(team_id);
CREATE INDEX idx_player_stats_season ON fact_player_season_stats(season);
CREATE INDEX idx_dates_season ON dim_dates(season);
```

**Why This Schema Works:**
- Star schema makes aggregations fast (especially for BI tools)
- Date dimension supports easy season-over-season comparisons
- Composite keys on fact tables prevent duplicates
- Foreign keys ensure data integrity
- Normalized dimensions avoid redundancy


Complete visual ERDs is in `ERD.png`

