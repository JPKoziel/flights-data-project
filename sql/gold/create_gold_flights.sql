-- ============================================
-- GOLD LAYER - Curated flights analytics
-- ============================================

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.airline_route_stats;

CREATE TABLE gold.airline_route_stats AS
SELECT
    -- Linia lotnicza i trasa
    f.airline                                           AS airline,
    f.airline_code                                      AS airline_code,
    f.origin                                            AS origin,
    f.origin_city                                       AS origin_city,
    f.dest                                              AS dest,
    f.dest_city                                         AS dest_city,

    -- Statystyki lotów
    COUNT(*)                                            AS total_flights,
    ROUND(AVG(f.dep_delay), 2)                         AS avg_dep_delay_min,
    ROUND(AVG(f.arr_delay), 2)                         AS avg_arr_delay_min,
    ROUND(AVG(f.distance), 2)                          AS avg_distance_miles,

    -- Opóźnienia według przyczyny
    ROUND(AVG(f.delay_due_carrier), 2)                 AS avg_carrier_delay,
    ROUND(AVG(f.delay_due_weather), 2)                 AS avg_weather_delay,
    ROUND(AVG(f.delay_due_nas), 2)                     AS avg_nas_delay,
    ROUND(AVG(f.delay_due_late_aircraft), 2)           AS avg_late_aircraft_delay,

    -- Odwołania i przekierowania
    SUM(f.cancelled)                                    AS total_cancelled,
    ROUND(100.0 * SUM(f.cancelled) / COUNT(*), 2)      AS cancellation_rate_pct,
    SUM(f.diverted)                                     AS total_diverted,

    -- Zakres dat
    MIN(f.fl_date)                                      AS first_flight_date,
    MAX(f.fl_date)                                      AS last_flight_date

FROM silver.flights f
GROUP BY
    f.airline,
    f.airline_code,
    f.origin,
    f.origin_city,
    f.dest,
    f.dest_city
HAVING
    COUNT(*) >= 10
    AND AVG(f.arr_delay) = AVG(f.arr_delay);  -- Filtruje NaN