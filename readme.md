MLB Analytics Platform
A data pipeline and analytics dashboard for Major League Baseball statistics, built with dbt, MotherDuck, and Evidence.dev.
Purpose
This project extracts player and team statistics from the MLB Stats API, transforms the data into a dimensional model with advanced baseball metrics, and presents the results through an interactive app.


Extract: Python script pulls 3 years of MLB data (2020-2022) from the official MLB Stats API
Load: Raw data is stored in MotherDuck cloud data warehouse
Transform: dbt models clean, structure, and calculate advanced metrics
Visualize: Evidence.dev dashboard provides interactive analytics

Data Model

Staging Layer: Cleaned and standardized raw data
Core Dimensions: Players, teams, seasons, positions
Fact Tables: Player hitting, pitching, and fielding statistics by season
Metrics Layer: Advanced sabermetrics (wOBA, FIP, ERA+, OPS+, etc.)
Reporting Views: Aggregated data for dashboard consumption

Tech Stack

Data Warehouse: MotherDuck (cloud DuckDB)
Data Transformation: dbt Core
ETL: Python with requests library (moving to dlt)
Viz: Evidence.dev

Key Features

40+ advanced baseball metrics and sabermetrics
Interactive filtering by season, team, player 
Team performance analysis with Pythagorean win expectation
Player comparison tools and league leader boards

Setup
bash# Install dependencies
pip install dbt-core dbt-duckdb
npm install -g @evidence-dev/evidence

# Set env variables
export MOTHERDUCK_TOKEN="your_token_here"

# Run dbt models
cd dbt
dbt run

# Start Evidence app
cd evidence  
npm run dev
Files Structure
├── dbt/                    # dbt project
│   ├── models/
│   │   ├── staging/        # Raw data cleanup
│   │   ├── intermediate/   # Business logic
│   │   ├── marts/          # Fact tables and metrics
│   │   └── reports/        # Dashboard-ready views
│   └── macros/             # Reusable SQL functions
├── evidence/               # Evidence.dev dashboard
│   └── pages/              # Dashboard pages
└── mlb_data_loader.py      # Data extraction script
