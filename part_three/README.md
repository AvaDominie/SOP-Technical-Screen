# Retrosheet Baseball Data Pipeline

A comprehensive ETL pipeline for extracting, transforming, and loading historical baseball data from [Retrosheet](https://www.retrosheet.org/) into a relational database optimized for analytics and visualization.

## 🎯 Project Overview

This project processes Retrosheet baseball statistics and creates a **Star Schema** data warehouse that supports:
- ✅ Season-over-season team performance analysis
- ✅ Player career trajectory tracking
- ✅ Historical trend analysis and forecasting
- ✅ Power BI / Tableau dashboard integration
- ✅ Time-series analysis with seasonal patterns

## 📊 Data Sources

### Primary Data Files (CSV Downloads)
- **gameinfo.csv** - Game-level metadata (dates, teams, scores, weather, umpires)
- **teamstats.csv** - Team batting, pitching, and defensive statistics per game
- **allplayers.csv** - Player season statistics and position appearances

### Reference Data Files
- **teams.csv** - Team master table (franchises, leagues, cities, nicknames)
- **ballparks.csv** - Stadium information (locations, operational dates)
- **biofile.csv** - Player biographical data (birth info, physical stats, HOF status)

## 🏗️ Database Schema

The database uses a **Star Schema** with:

### Dimension Tables
- `dim_teams` - Team/franchise master
- `dim_players` - Player biographical information
- `dim_ballparks` - Stadium master
- `dim_dates` - Date dimension for time-series analysis

### Fact Tables
- `fact_games` - Game-level results and metadata
- `fact_team_game_stats` - Team batting/pitching/defensive stats per game
- `fact_player_season_stats` - Player season aggregated statistics

📖 See [SCHEMA_DESIGN.md](SCHEMA_DESIGN.md) for detailed documentation.

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- pip (Python package manager)
- Optional: PostgreSQL (SQLite used by default)

### Installation

1. **Clone or download this repository**

2. **Create virtual environment**
   ```powershell
   py -m venv .venv
   .venv\Scripts\Activate.ps1
   ```

3. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```powershell
   cp .env.example .env
   # Edit .env with your database settings
   ```

5. **Download Retrosheet data**
   
   Create a `data/` directory and download files from:
   - Game logs: https://www.retrosheet.org/gamelogs/index.html
   - Teams/Parks: https://www.retrosheet.org/
   
   Place CSV files in the `data/` directory:
   ```
   data/
   ├── gameinfo.csv
   ├── teamstats.csv
   ├── allplayers.csv
   ├── teams.csv
   ├── ballparks.csv
   └── biofile.csv
   ```

### Running the ETL Pipeline

```powershell
python src/data_ingestion.py
```

This will:
1. ✅ Extract data from CSV files
2. ✅ Clean and transform the data
3. ✅ Create database schema
4. ✅ Load dimension tables
5. ✅ Load fact tables
6. ✅ Create a `retrosheet.db` SQLite database (or PostgreSQL if configured)

## 📁 Project Structure

```
part_three/
├── src/
│   └── data_ingestion.py      # Main ETL pipeline script
├── data/                       # CSV data files (not in git)
├── .venv/                      # Virtual environment (not in git)
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (not in git)
├── .env.example                # Environment template
├── .gitignore                  # Git ignore rules
├── README.md                   # This file
├── SCHEMA_DESIGN.md            # Detailed schema documentation
└── notes                       # Project notes
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Database type: 'sqlite' or 'postgresql'
DB_TYPE=sqlite

# PostgreSQL settings (if DB_TYPE=postgresql)
DB_USER=postgres
DB_PASS=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retrosheet

# Data directory
DATA_DIR=./data
```

### Using PostgreSQL (Production)

1. Install PostgreSQL
2. Create database: `CREATE DATABASE retrosheet;`
3. Update `.env` with connection details
4. Set `DB_TYPE=postgresql`
5. Run ETL pipeline

## 📊 Sample Queries

### Season Win/Loss Records
```sql
SELECT 
    d.season,
    t.full_name,
    SUM(CASE WHEN fg.winning_team_id = t.team_id THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN fg.losing_team_id = t.team_id THEN 1 ELSE 0 END) as losses
FROM fact_games fg
JOIN dim_dates d ON fg.date_id = d.date_id
JOIN dim_teams t ON (fg.home_team_id = t.team_id OR fg.visitor_team_id = t.team_id)
WHERE d.season = 1898
GROUP BY d.season, t.full_name
ORDER BY wins DESC;
```

### Player Career Positions
```sql
SELECT 
    p.first_name || ' ' || p.last_name as player,
    fps.season,
    fps.games,
    fps.games_pitcher,
    fps.games_catcher,
    fps.games_outfield
FROM fact_player_season_stats fps
JOIN dim_players p ON fps.player_id = p.player_id
WHERE fps.games > 100
ORDER BY fps.season, fps.games DESC;
```

### Home Field Advantage
```sql
SELECT 
    t.full_name,
    COUNT(*) as home_games,
    SUM(CASE WHEN fg.home_runs > fg.visitor_runs THEN 1 ELSE 0 END) as home_wins,
    ROUND(100.0 * SUM(CASE WHEN fg.home_runs > fg.visitor_runs THEN 1 ELSE 0 END) / COUNT(*), 2) as win_pct
FROM fact_games fg
JOIN dim_teams t ON fg.home_team_id = t.team_id
GROUP BY t.full_name
HAVING COUNT(*) > 50
ORDER BY win_pct DESC;
```

## 📈 Data Quality & Validation

The ETL pipeline includes:
- ✅ Data type validation
- ✅ Null value handling
- ✅ Referential integrity checks
- ✅ Statistical validation (e.g., hits ≤ at-bats)
- ✅ Duplicate removal (stattype='official' records only)
- ✅ Logging at each pipeline stage

## 🔮 Analytics Use Cases

### 1. Time-Series Analysis
- Seasonal trend detection
- Performance forecasting with Prophet
- Year-over-year comparisons
- Monthly/weekly patterns

### 2. Team Performance
- Win/loss streaks
- Home vs. away performance
- Division/league standings
- Head-to-head matchups

### 3. Player Analysis
- Career trajectories
- Position versatility
- Breakout seasons
- Hall of Fame comparisons

### 4. Environmental Factors
- Weather impact on scoring
- Day vs. night games
- Ballpark effects
- Attendance trends

## 📊 BI Tool Integration

### Power BI
1. Connect to SQLite/PostgreSQL database
2. Import dimension and fact tables
3. Create relationships (auto-detect from schema)
4. Build calculated measures:
   - Batting Average = SUM(hits) / SUM(at_bats)
   - ERA = (SUM(earned_runs) * 9) / (SUM(ip_outs) / 3)
   - OPS = On-Base % + Slugging %

### Tableau
1. Connect to database
2. Import tables as data source
3. Create calculated fields
4. Build dashboards with filters by season, team, player

## 🎁 Bonus Features

### Time-Series Forecasting (Prophet)
```python
from prophet import Prophet
import pandas as pd

# Example: Forecast team wins
df = pd.read_sql("""
    SELECT d.full_date as ds, COUNT(*) as y
    FROM fact_games fg
    JOIN dim_dates d ON fg.date_id = d.date_id
    WHERE fg.winning_team_id = 'CIN'
    GROUP BY d.full_date
""", engine)

model = Prophet()
model.fit(df)
future = model.make_future_dataframe(periods=365)
forecast = model.predict(future)
```

### Correlation Analysis
```python
import numpy as np
from scipy.stats import pearsonr

# Temperature vs. Total Runs
query = """
SELECT temperature, (home_runs + visitor_runs) as total_runs
FROM fact_games
WHERE temperature IS NOT NULL
"""
df = pd.read_sql(query, engine)
corr, p_value = pearsonr(df['temperature'], df['total_runs'])
```

## 🐛 Troubleshooting

### Common Issues

**Issue:** `ModuleNotFoundError: No module named 'pandas'`  
**Solution:** Activate virtual environment and run `pip install -r requirements.txt`

**Issue:** `FileNotFoundError: data/gameinfo.csv not found`  
**Solution:** Download CSV files from Retrosheet and place in `data/` directory

**Issue:** `Database connection error`  
**Solution:** Check `.env` configuration and ensure PostgreSQL is running (if using)

## 📚 References

- [Retrosheet Official Site](https://www.retrosheet.org/)
- [Retrosheet Game Logs](https://www.retrosheet.org/gamelogs/index.html)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## 📄 License

This project is for educational/demonstration purposes. Retrosheet data is provided freely but with specific [terms of use](https://www.retrosheet.org/notice.txt).

## 👤 Author

Created as part of a technical screening assessment.

---

**Last Updated:** January 2026  
**Python Version:** 3.8+  
**Database:** SQLite / PostgreSQL
