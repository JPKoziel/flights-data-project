-- ============================================
-- SILVER LAYER - Cleaned flights data
-- ============================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.flights;

CREATE TABLE silver.flights AS
SELECT DISTINCT
    -- Daty i czas
    FL_DATE::DATE                                       AS fl_date,
    AIRLINE                                             AS airline,
    AIRLINE_CODE                                        AS airline_code,
    FL_NUMBER::INTEGER                                  AS fl_number,

    -- Lotniska
    ORIGIN                                              AS origin,
    ORIGIN_CITY                                         AS origin_city,
    DEST                                                AS dest,
    DEST_CITY                                           AS dest_city,

    -- Czasy
    CRS_DEP_TIME::INTEGER                               AS crs_dep_time,
    NULLIF(DEP_TIME, 'NaN')::NUMERIC                    AS dep_time,
    NULLIF(DEP_DELAY, 'NaN')::NUMERIC                   AS dep_delay,
    CRS_ARR_TIME::INTEGER                               AS crs_arr_time,
    NULLIF(ARR_TIME, 'NaN')::NUMERIC                    AS arr_time,
    NULLIF(ARR_DELAY, 'NaN')::NUMERIC                   AS arr_delay,

    -- Dystans i czas lotu
    NULLIF(DISTANCE, 'NaN')::NUMERIC                    AS distance,
    NULLIF(CRS_ELAPSED_TIME, 'NaN')::NUMERIC            AS crs_elapsed_time,
    NULLIF(ELAPSED_TIME, 'NaN')::NUMERIC                AS elapsed_time,
    NULLIF(AIR_TIME, 'NaN')::NUMERIC                    AS air_time,

    -- Opóźnienia według przyczyny
    NULLIF(DELAY_DUE_CARRIER, 'NaN')::NUMERIC           AS delay_due_carrier,
    NULLIF(DELAY_DUE_WEATHER, 'NaN')::NUMERIC           AS delay_due_weather,
    NULLIF(DELAY_DUE_NAS, 'NaN')::NUMERIC               AS delay_due_nas,
    NULLIF(DELAY_DUE_SECURITY, 'NaN')::NUMERIC          AS delay_due_security,
    NULLIF(DELAY_DUE_LATE_AIRCRAFT, 'NaN')::NUMERIC     AS delay_due_late_aircraft,

    -- Status lotu
    NULLIF(CANCELLED, 'NaN')::NUMERIC                   AS cancelled,
    CANCELLATION_CODE                                   AS cancellation_code,
    NULLIF(DIVERTED, 'NaN')::NUMERIC                    AS diverted

FROM bronze.flights
WHERE
    FL_DATE IS NOT NULL
    AND AIRLINE IS NOT NULL
    AND ORIGIN IS NOT NULL
    AND DEST IS NOT NULL
    AND FL_DATE ~ '^\d{4}-\d{2}-\d{2}$';