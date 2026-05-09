-- ============================================
-- Data Quality Metrics — silver.flights
-- Run after each pipeline execution
-- ============================================

SELECT
    'completeness_arr_delay'        AS metric_name,
    ROUND(100.0 * COUNT(arr_delay) / COUNT(*), 2)::TEXT || '%' AS value,
    '> 95%'                         AS threshold,
    CASE WHEN ROUND(100.0 * COUNT(arr_delay) / COUNT(*), 2) >= 95
         THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver.flights

UNION ALL

SELECT
    'completeness_airline',
    ROUND(100.0 * COUNT(airline) / COUNT(*), 2)::TEXT || '%',
    '= 100%',
    CASE WHEN COUNT(airline) = COUNT(*) THEN 'PASS' ELSE 'FAIL' END
FROM silver.flights

UNION ALL

SELECT
    'uniqueness_flight_records',
    ROUND(100.0 * COUNT(DISTINCT (fl_date, airline_code, origin, dest, fl_number)) / COUNT(*), 2)::TEXT || '%',
    '= 100%',
    CASE WHEN COUNT(DISTINCT (fl_date, airline_code, origin, dest, fl_number)) = COUNT(*)
         THEN 'PASS' ELSE 'FAIL' END
FROM silver.flights

UNION ALL

SELECT
    'row_count',
    COUNT(*)::TEXT,
    '> 2900000',
    CASE WHEN COUNT(*) > 2900000 THEN 'PASS' ELSE 'FAIL' END
FROM silver.flights

UNION ALL

SELECT
    'validity_arr_delay',
    ROUND(100.0 * SUM(CASE WHEN arr_delay BETWEEN -120 AND 1440 THEN 1 ELSE 0 END) / COUNT(arr_delay), 2)::TEXT || '%',
    '> 99%',
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN arr_delay BETWEEN -120 AND 1440 THEN 1 ELSE 0 END) / COUNT(arr_delay), 2) >= 99
         THEN 'PASS' ELSE 'FAIL' END
FROM silver.flights

UNION ALL

SELECT
    'data_freshness',
    MAX(fl_date)::TEXT,
    'most recent date',
    'INFO'
FROM silver.flights;