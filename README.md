# ✈️ US Flight Delays - Data Engineering Project

## Problem Statement
This project analyzes US domestic flight delays and cancellations (2019-2023) 
to identify which airlines and routes have the worst on-time performance.
The goal is to build a reliable data pipeline using the Medallion Architecture
(Bronze → Silver → Gold) that transforms raw flight data into actionable analytics.

**Key analytical question:**  
*Which airlines and routes have the highest average arrival delays 
and cancellation rates?*

## Architecture
The project follows the **Medallion Architecture**:

| Layer | Schema | Description |
|---|---|---|
| Bronze | `bronze.flights` | Raw data loaded 1:1 from CSV |
| Silver | `silver.flights` | Cleaned data with correct types, NaN removed |
| Gold | `gold.airline_route_stats` | Aggregated route statistics with JOINs |

## Tech Stack
- **Database:** PostgreSQL 15 (Docker)
- **Language:** Python 3.12, SQL
- **Tools:** Git, Docker, VS Code

## How to Run

### 1. Requirements
- Docker Desktop
- Python 3.12+
- PostgreSQL running on port 5432

### 2. Download the dataset
Download the dataset manually from Kaggle:  
👉 https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023

Place the file in `data/raw/flights_sample_3m.csv`

### 3. Setup environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary pandas
```

### 4. Start PostgreSQL
```bash
docker run --name flights-postgres \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=flights_db \
  -p 5432:5432 -d postgres:15
```

### 5. Run the pipeline
```bash
# Load Bronze layer
python3 scripts/load_bronze.py

# Load Silver layer
docker cp sql/silver/create_silver_flights.sql flights-postgres:/tmp/
docker exec flights-postgres psql -U admin -d flights_db -f /tmp/create_silver_flights.sql

# Load Gold layer
docker cp sql/gold/create_gold_flights.sql flights-postgres:/tmp/
docker exec flights-postgres psql -U admin -d flights_db -f /tmp/create_gold_flights.sql
```

## Data Quality Risks
See [docs/data_quality_risks.md](docs/data_quality_risks.md)

## Repository Structure
```
flights-data-project/
├── data/
│   └── raw/          ← CSV files (not tracked by Git)
├── docs/
│   └── data_quality_risks.md
├── scripts/
│   └── load_bronze.py
├── sql/
│   ├── silver/
│   │   └── create_silver_flights.sql
│   └── gold/
│       └── create_gold_flights.sql
└── README.md
```