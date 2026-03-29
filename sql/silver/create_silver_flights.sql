-- ============================================
-- SILVER LAYER - Cleaned flights data
-- ============================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.flights;

CREATE TABLE silver.flights AS
SELECT DISTINCT
    -- Daty i czas
    FL_DATE::DATE                           AS fl_date,
    AIRLINE                                 AS airline,
    AIRLINE_CODE                            AS airline_code,
    FL_NUMBER::INTEGER                      AS fl_number,
    
    -- Lotniska
    ORIGIN                                  AS origin,
    ORIGIN_CITY                             AS origin_city,
    DEST                                    AS dest,
    DEST_CITY                               AS dest_city,
    
    -- Czasy
    CRS_DEP_TIME::INTEGER                   AS crs_dep_time,
    DEP_TIME::NUMERIC                       AS dep_time,
    DEP_DELAY::NUMERIC                      AS dep_delay,
    CRS_ARR_TIME::INTEGER                   AS crs_arr_time,
    ARR_TIME::NUMERIC                       AS arr_time,
    ARR_DELAY::NUMERIC                      AS arr_delay,
    
    -- Dystans i czas lotu
    DISTANCE::NUMERIC                       AS distance,
    CRS_ELAPSED_TIME::NUMERIC               AS crs_elapsed_time,
    ELAPSED_TIME::NUMERIC                   AS elapsed_time,
    AIR_TIME::NUMERIC                       AS air_time,
    
    -- Opóźnienia
    DELAY_DUE_CARRIER::NUMERIC              AS delay_due_carrier,
    DELAY_DUE_WEATHER::NUMERIC              AS delay_due_weather,
    DELAY_DUE_NAS::NUMERIC                  AS delay_due_nas,
    DELAY_DUE_SECURITY::NUMERIC             AS delay_due_security,
    DELAY_DUE_LATE_AIRCRAFT::NUMERIC        AS delay_due_late_aircraft,
    
    -- Status lotu
    CANCELLED::NUMERIC                      AS cancelled,
    CANCELLATION_CODE                       AS cancellation_code,
    DIVERTED::NUMERIC                       AS diverted

FROM bronze.flights
WHERE
    -- Usuwamy wiersze bez kluczowych danych
    FL_DATE IS NOT NULL
    AND AIRLINE IS NOT NULL
    AND ORIGIN IS NOT NULL
    AND DEST IS NOT NULL
    AND FL_DATE ~ '^\d{4}-\d{2}-\d{2}$';  -- Tylko poprawny format daty