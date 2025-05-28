# MLB Analytics Platform

A data pipeline and analytics dashboard for Major League Baseball statistics, built with dbt, MotherDuck, and Evidence.dev.
Purpose
This project extracts player and team statistics from the MLB Stats API, transforms the data into a dimensional model with advanced baseball metrics, and presents the results through an interactive app.

# Data Model

- Staging Layer: Cleaned and standardized raw data
- Core Dimensions: Players, teams, seasons, positions
- Fact Tables: Player hitting, pitching, and fielding statistics by season
- Metrics Layer: Advanced sabermetrics (wOBA, FIP, ERA+, OPS+, etc.)
- Reporting Views: Aggregated data for dashboard consumption

# Tech Stack

- Data Warehouse: MotherDuck (cloud DuckDB)
- Data Transformation: dbt Core
- ETL: Python with requests library (moving to dlt)
- Viz: Evidence.dev

# Key Features

40+ advanced baseball metrics and sabermetrics
Interactive filtering by season, team, player 
Team performance analysis with Pythagorean win expectation
Player comparison tools and league leader boards



