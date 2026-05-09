# Data Quality Metrics — US Flight Delays

Metrics are calculated on the **silver.flights** table after each pipeline run.
Last calculated: 2024-01-01 (recalculate after each run using `sql/metrics/calculate_metrics.sql`)

---

## Metrics Summary

| Metric | Definition | Current Value | Expected Threshold | Update Cadence |
|---|---|---|---|---|
| Completeness of `arr_delay` | % of rows where `arr_delay` is not NULL | 97.13% | > 95% | Every pipeline run |
| Completeness of `airline` | % of rows where `airline` is not NULL | 100.00% | 100% | Every pipeline run |
| Uniqueness of flight records | % of unique (fl_date, airline_code, origin, dest, fl_number) combinations | 100.00% | 100% | Every pipeline run |
| Row count | Total number of records in silver.flights | 3,000,000 | > 2,900,000 | Every pipeline run |
| Validity of `arr_delay` | % of arr_delay values between -120 and 1440 minutes | 100.00% | > 99% | Every pipeline run |
| Data freshness | Most recent flight date in the dataset | 2023-08-31 | < 90 days from today | Daily |

---

## Metric Definitions

### 1. Completeness of `arr_delay` (97.13%)
**Definition:** Percentage of rows where arrival delay is not NULL.
**Why it matters:** NULL values in arr_delay indicate cancelled or diverted flights where delay could not be measured. 2.87% missing is expected and acceptable — these are cancelled flights.
**Threshold:** > 95% — if completeness drops below this, it may indicate a data ingestion issue.

### 2. Completeness of `airline` (100.00%)
**Definition:** Percentage of rows where airline name is not NULL.
**Why it matters:** Every flight must be attributed to an airline. Missing airline data makes the record unusable for analysis.
**Threshold:** 100% — any missing airline is a critical data quality issue.

### 3. Uniqueness of flight records (100.00%)
**Definition:** Percentage of unique (fl_date, airline_code, origin, dest, fl_number) combinations out of total rows.
**Why it matters:** Duplicate flight records would inflate delay statistics and route counts in the gold layer.
**Threshold:** 100% — any duplicates indicate a pipeline idempotency failure.

### 4. Row count (3,000,000)
**Definition:** Total number of records loaded into silver.flights.
**Why it matters:** Sudden drops in row count indicate data loss during ingestion or transformation.
**Threshold:** > 2,900,000 — a drop of more than 100,000 rows triggers investigation.

### 5. Validity of `arr_delay` (100.00%)
**Definition:** Percentage of arr_delay values that fall within the range [-120, 1440] minutes.
**Why it matters:** Arrival delays below -120 minutes or above 1440 minutes (24 hours) are physically implausible and indicate data corruption.
**Threshold:** > 99% — values outside this range should be rare anomalies.

### 6. Data freshness (2023-08-31)
**Definition:** The most recent flight date present in the dataset.
**Why it matters:** Stale data reduces the analytical value of the product.
**Threshold:** < 90 days from today — this dataset covers 2019-2023 so freshness is static; for a live pipeline this would be monitored daily.

---

## How to recalculate

```sql
-- Run this after each pipeline execution:
\i sql/metrics/calculate_metrics.sql
```

Or via terminal:
```bash
docker exec postgres_db psql -U admin -d flights_db -f /tmp/calculate_metrics.sql
```