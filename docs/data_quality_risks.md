# Data Quality Risks

## 1. NaN Values in Numeric Columns (Ingestion Problem)
**Layer:** Bronze → Silver  
**Description:** Numeric columns such as `arr_delay`, `dep_delay`, and `air_time` contain NaN values instead of NULL for cancelled flights. PostgreSQL treats NaN as a valid numeric value, which causes incorrect aggregations (averages, sums) in downstream layers.  
**Detected:** 86,198 rows with NaN in the `arr_delay` column  
**Solution:** In the Silver layer, `NULLIF(column, 'NaN')::NUMERIC` was applied to all numeric columns to convert NaN to NULL before transformation.

## 2. Missing Delay Cause Data (Transformation Problem)
**Layer:** Silver → Gold  
**Description:** Delay cause columns (`delay_due_carrier`, `delay_due_weather`, `delay_due_nas`, `delay_due_security`, `delay_due_late_aircraft`) are empty for flights that were not delayed or were cancelled. This can lead to misinterpretation — a missing value does not mean there was no delay, it may simply mean the data was never recorded.  
**Detected:** A significant portion of rows have NULL in all delay_due_* columns  
**Solution:** In the Gold layer, AVG() aggregations naturally ignore NULL values, producing correct averages only for flights where delay cause data exists.

## 3. Small Sample Bias on Routes (Scale Problem)
**Layer:** Gold  
**Description:** Some routes have very few flights (10-20 records), making their average delay statistics statistically unreliable. A route with only 13 flights and an average delay of 247 minutes may represent an anomaly or data error rather than a true operational pattern.  
**Detected:** Multiple routes in `gold.airline_route_stats` have fewer than 50 flights  
**Solution:** A minimum threshold of `HAVING COUNT(*) >= 10` was applied. For production-grade analysis, this threshold should be increased to 100+ flights to ensure statistical significance.