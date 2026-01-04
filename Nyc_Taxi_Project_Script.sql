-- ===========================================================
-- DATABASE & SCHEMAS
-- ===========================================================
CREATE DATABASE nyc_taxi;
GO
USE nyc_taxi;
GO

CREATE SCHEMA raw;
GO
CREATE SCHEMA clean;
GO
CREATE SCHEMA analytics;
GO

-- ===========================================================
-- RAW TABLES
-- ===========================================================

CREATE TABLE raw.taxi_trips (
    VendorID VARCHAR(10),
    tpep_pickup_datetime VARCHAR(50),
    tpep_dropoff_datetime VARCHAR(50),
    passenger_count VARCHAR(10),
    trip_distance VARCHAR(50),   
    RatecodeID VARCHAR(10),
    store_and_fwd_flag VARCHAR(5),
    PULocationID VARCHAR(10),
    DOLocationID VARCHAR(10),
    payment_type VARCHAR(10),
    fare_amount VARCHAR(50),
    extra VARCHAR(50),
    mta_tax VARCHAR(50),
    tip_amount VARCHAR(50),
    tolls_amount VARCHAR(50),
    improvement_surcharge VARCHAR(50),
    total_amount VARCHAR(50),
    congestion_surcharge VARCHAR(50),
    airport_fee VARCHAR(50)
);


CREATE TABLE raw.taxi_zones (
    Zone NVARCHAR(100),
    LocationID INT,
    Borough NVARCHAR(50)
);

-- ===========================================================
-- BULK INSERT – LOAD RAW DATA
-- ===========================================================
-- Load Taxi Trip Data
BULK INSERT raw.taxi_trips
FROM 'D:\Ehtsham\Project\New York Taxi\2023_Yellow_Taxi_Trip_Data.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    ERRORFILE = 'D:\Ehtsham\Project\New York Taxi\bulk_trip_errors.log'
);

-- Load Taxi Zone Data
BULK INSERT raw.taxi_zones
FROM 'D:\Ehtsham\Project\New York Taxi\NYC_Taxi_Zones_CLEAN.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    ERRORFILE = 'D:\Ehtsham\Project\New York Taxi\bulk_zone_errors.log'
);
Select Count(*) From raw.taxi_trips;
Select Count(*) From raw.taxi_zones;
-- 38310226 rows are inserted in raw.taxi_trips_raw.
-- 238 rows are inserted in raw.taxi_trips_raw.


-- ===========================================================
-- DATA QUALITY CHECKS
-- ===========================================================
-- Invalid timestamps
SELECT COUNT(*)
FROM raw.taxi_trips
WHERE tpep_dropoff_datetime <= tpep_pickup_datetime;

-- 825577 rows have invalid timestamps.

--------------------------------------------------------------
-- Invalid fare & distance
SELECT
    SUM(CASE WHEN TRY_CAST(fare_amount AS FLOAT) <= 0 THEN 1 ELSE 0 END) AS invalid_fare,
    SUM(CASE WHEN TRY_CAST(trip_distance AS FLOAT) <= 0 THEN 1 ELSE 0 END) AS invalid_distance
FROM raw.taxi_trips;

-- 394503 rows have invalid fare and 773457 rows have invalid distance.

--------------------------------------------------------------
-- Invalid zones
SELECT COUNT(*)
FROM raw.taxi_trips t
LEFT JOIN raw.taxi_zones pu ON t.PULocationID = pu.LocationID
LEFT JOIN raw.taxi_zones doo ON t.DOLocationID = doo.LocationID
WHERE pu.LocationID IS NULL OR doo.LocationID IS NULL;

-- 3138481 rows have invalid zones.

-- ===========================================================
-- CLEAN & DEDUPLICATED TABLE
-- ===========================================================
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   tpep_pickup_datetime,
                   tpep_dropoff_datetime,
                   PULocationID,
                   DOLocationID,
                   fare_amount
               ORDER BY tpep_pickup_datetime
           ) AS rn
    FROM raw.taxi_trips
)
SELECT
    CAST(VendorID AS INT) AS VendorID,
    CAST(tpep_pickup_datetime AS DATETIME2) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS DATETIME2) AS dropoff_datetime,
    CAST(PULocationID AS INT) AS PULocationID,
    z1.Zone AS PUZone,
    CAST(DOLocationID AS INT) AS DOLocationID,
    z2.Zone AS DOZone,
    TRY_CAST(trip_distance AS FLOAT) AS trip_distance,
    TRY_CAST(fare_amount AS FLOAT) AS fare_amount,
    TRY_CAST(total_amount AS FLOAT) AS total_amount,
    TRY_CAST(passenger_count AS INT) AS passenger_count,
    DATEDIFF( MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)
    ) AS trip_duration_min
INTO clean.taxi_trips
FROM deduped d
JOIN raw.taxi_zones z1
  ON TRY_CAST(d.PULocationID AS INT) = z1.LocationID
JOIN raw.taxi_zones z2
  ON TRY_CAST(d.DOLocationID AS INT) = z2.LocationID
WHERE rn = 1
  AND TRY_CAST(fare_amount AS FLOAT) > 0
  AND TRY_CAST(trip_distance AS FLOAT) > 0
  AND CAST(tpep_dropoff_datetime AS DATETIME2)
      > CAST(tpep_pickup_datetime AS DATETIME2);

-- After clean up total rows are 34230429 Out of 38310226
-- 89.35 % data is have valid rows.

-- ===========================================================
-- ENRICHED ANALYTICAL VIEW
-- ===========================================================
CREATE OR ALTER VIEW analytics.taxi_trips_enriched AS 
SELECT
    pickup_datetime,
    PULocationID,
    PUZone,
    DOLocationID,
    DOZone,
    trip_distance,
    trip_duration_min,
    fare_amount,
    DATEPART(WEEKDAY, pickup_datetime) AS pickup_weekday,
    DATEPART(HOUR, pickup_datetime) AS pickup_hour,
    CAST(pickup_datetime AS DATE) AS pickup_date,

    CASE
        WHEN DATEPART(WEEKDAY, pickup_datetime) IN (1, 7)
        THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,

    CASE
        WHEN DATEPART(HOUR, pickup_datetime) BETWEEN 5 AND 10 THEN 'Morning'
        WHEN DATEPART(HOUR, pickup_datetime) BETWEEN 11 AND 15 THEN 'Afternoon'
        WHEN DATEPART(HOUR, pickup_datetime) BETWEEN 16 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS day_part,

    CASE
        WHEN trip_distance < 2 THEN 'Short'
        WHEN trip_distance BETWEEN 2 AND 8 THEN 'Medium'
        ELSE 'Long'
    END AS trip_length_type

FROM clean.taxi_trips;

-- ===========================================================
-- HIGH-DEMAND PICKUP WINDOWS
-- (Average Trips per Day-Hour by Zone)
-- ===========================================================
SELECT
    pickup_weekday,
    pickup_hour,
    PUZone,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date), 2) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY pickup_weekday, pickup_hour, PUZone
ORDER BY avg_trips_per_day_hour DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------

-- 1. Midtown Center has the highest pickup demand during
--    weekday evening hours (5–7 PM), mainly due to office
--    commute traffic and tourist movement.
--
-- 2. East Village shows strong late-night demand on weekends,
--    especially after midnight, driven by nightlife activity.
--
-- 3. Upper East Side maintains steady pickup demand across
--    most hours of the day, reflecting consistent residential
--    travel rather than short demand spikes.

-- -----------------------------------------------------------


-- ===========================================================
-- TRAFFIC CONGESTION IMPACT ON DRIVER TIME EFFICIENCY
-- (Zones and Hours with Highest Minutes per Mile)
-- ===========================================================
SELECT
    pickup_weekday,
    pickup_hour,
    PUZone,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date), 2) AS avg_trips_per_day_hour,
    ROUND(AVG(trip_duration_min), 2) AS avg_trip_duration_min,
    ROUND(
        AVG(trip_duration_min / NULLIF(trip_distance, 0)),
        2
    ) AS minutes_per_mile
FROM analytics.taxi_trips_enriched
GROUP BY pickup_weekday, pickup_hour, PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) > 100.00
    AND AVG(trip_distance) >= 1.0
ORDER BY minutes_per_mile DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------

-- 1. Times Square / Theatre District, Penn Station / Madison Sq West,
--    Midtown Center, and East Chelsea consistently show the highest
--    minutes-per-mile, indicating ongoing traffic congestion rather
--    than occasional delays.

-- 2. Transit-heavy areas like Penn Station appear across many days
--    and hours, showing that congestion in these zones is structural
--    and not limited to rush hours.

-- 3. Evening hours (around 5 PM–8 PM) are the most time-inefficient,
--    as high pickup demand coincides with very slow vehicle movement.

-- 4. LaGuardia Airport shows congestion driven mainly by access delays
--    and long trip durations, not dense city traffic.

-- 5. Some zones, such as Midtown Center and parts of the Upper East Side,
--    maintain high demand with relatively lower minutes-per-mile,
--    indicating better traffic flow despite activity.

-- 6. Congestion patterns remain consistent across days and hours,
--    meaning traffic inefficiency is predictable and can be planned
--    for in driver positioning strategies.

-- -----------------------------------------------------------


-- ===========================================================
-- DRIVER EARNINGS PER HOUR
-- (Income Efficiency by Day, Hour, and Zone)
-- ===========================================================
SELECT
    pickup_weekday,
    pickup_hour,
    PUZone,
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY pickup_weekday, pickup_hour, PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 50
ORDER BY earnings_per_hour DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. Airport pickup windows, especially LaGuardia Airport,
--    deliver the highest earnings per hour across many days
--    and hours, driven by longer and higher-fare trips.
--
-- 2. Non-airport zones such as the Garment District and
--    West Village also achieve very high earnings per hour,
--    but only during specific time windows.
--
-- 3. Late-night and early-morning hours (roughly 10 PM–6 AM)
--    produce the strongest income efficiency due to lower
--    traffic congestion.
--
-- 4. High earnings per hour often occur even with moderate
--    trip volume, showing that trip value matters more than
--    number of trips.
--
-- 5. Midtown Center and Upper East Side appear frequently in
--    high-earning early-morning windows, supported by smooth
--    traffic and efficient short-to-medium trips.
--
-- 6. The repeated presence of LaGuardia Airport across many
--    day-hour combinations shows that airport demand is
--    consistently profitable, not limited to a single peak.
--
-- -----------------------------------------------------------


-- ===========================================================
-- DEMAND VS EARNINGS GAP
-- (High Pickup Volume with Low Income Efficiency)
-- ===========================================================
SELECT
    pickup_weekday,
    pickup_hour,
    PUZone,

    -- Demand signal (normalized per day)
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour,

    -- Income efficiency
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour
FROM analytics.taxi_trips_enriched
GROUP BY pickup_weekday, pickup_hour, PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 100
ORDER BY earnings_per_hour ASC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. Central Park, Penn Station / Madison Sq West,
--    Union Square, and East Chelsea consistently show
--    very high pickup demand but low earnings per hour.
--
-- 2. Central Park records some of the highest trip volumes
--    across many days and hours, yet earnings per hour
--    remain among the lowest, mainly due to short trips
--    and low fare value.
--
-- 3. Transit- and pedestrian-heavy areas such as
--    Penn Station and Union Square generate constant demand,
--    but congestion and slow trip turnover reduce income efficiency.
--
-- 4. Zones like East Chelsea and West Chelsea / Hudson Yards
--    demonstrate that high activity does not automatically
--    convert into higher income.
--
-- 5. These zones stand in clear contrast to airport pickups,
--    which show lower demand but much higher earnings per hour.
--
-- 6. The repeated appearance of the same zones across
--    multiple days and hours indicates a consistent,
--    structural mismatch between demand and income.
--
-- -----------------------------------------------------------


-- ===========================================================
-- INCOME STABILITY AND RISK ANALYSIS
-- (Earnings Consistency by Day, Hour, and Zone)
-- ===========================================================
SELECT
    pickup_weekday,
    pickup_hour,
    PUZone,

    -- Expected income rate
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour,

    -- Income risk proxy (trip duration volatility)
    ROUND(
        STDEV(trip_duration_min),
        2
    ) AS trip_duration_volatility_min,

    -- Reliability of demand
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY pickup_weekday, pickup_hour, PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 50
ORDER BY
    earnings_per_hour DESC,
    trip_duration_volatility_min ASC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. Airport-related pickup windows appear frequently with
--    high earnings per hour and relatively low trip-duration
--    volatility, indicating stable and predictable income.
--
-- 2. Some zones deliver very high earnings per hour but also
--    show high volatility in trip duration, meaning income
--    is less predictable despite strong returns.
--
-- 3. Several zones achieve moderate earnings per hour with
--    very low volatility, making them reliable and low-risk
--    earning options.
--
-- 4. High pickup demand does not always mean stable income.
--    Some high-demand windows still show large variation in
--    trip duration, increasing income uncertainty.
--
-- 5. Zones with similar earnings per hour can differ greatly
--    in volatility, showing that income rate alone is not
--    sufficient to evaluate driver performance.
--
-- 6. The best earning windows balance three factors:
--    strong earnings per hour, sufficient demand, and
--    low trip-duration volatility, offering the best
--    overall risk–reward tradeoff.
--
-- -----------------------------------------------------------


-- INTERPRETIVE (AGGREGATED) ANALYSIS

-- ===========================================================
-- WEEKDAY VS WEEKEND INCOME EFFICIENCY
-- (How Earnings Change by Day Type, Hour, and Zone)
-- ===========================================================
SELECT
    day_type,
    pickup_weekday,
    pickup_hour,
    PUZone,

    -- Income efficiency
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour,

    -- Demand normalized per day
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY
    day_type,
    pickup_weekday,
    pickup_hour,
    PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 50
ORDER BY
    earnings_per_hour DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. The highest earnings per hour occur mainly on weekdays,
--    showing that weekday travel patterns are more income-efficient
--    than weekends.
--
-- 2. Airport zones appear at the top on both weekdays and weekends,
--    confirming that airport trips are consistently profitable.
--
-- 3. Weekends still have some high-earning windows, but these are
--    fewer and usually pay slightly less than weekday peaks.
--
-- 4. High demand alone does not guarantee high earnings.
--    Even airport zones show earnings variation by hour.
--
-- 5. Some non-airport zones achieve very high earnings, but only
--    during narrow weekday time windows.
--
-- 6. Earnings depend strongly on the combination of day type,
--    hour, and zone, not on day type alone.
--
-- -----------------------------------------------------------


-- ===========================================================
-- DAY PART PERFORMANCE
-- (Income Efficiency by Time of Day, Zone, and Hour)
-- ===========================================================
SELECT
    day_part,
    pickup_weekday,
    pickup_hour,
    PUZone,

    -- Income efficiency
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour,

    -- Demand normalized per day
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY
    day_part,
    pickup_weekday,
    pickup_hour,
    PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 50
ORDER BY
    earnings_per_hour DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. Afternoon hours can produce extremely high earnings,
--    but only in very specific zone–hour combinations.
--
-- 2. Night hours appear most frequently among top earners,
--    especially for airport-related pickups, due to lower traffic.
--
-- 3. Morning hours show stable demand across many zones,
--    but earnings per hour are generally moderate.
--
-- 4. Evening hours balance demand and earnings, but usually
--    do not outperform Night or peak Afternoon windows.
--
-- 5. High demand does not always lead to high earnings.
--    Some high-demand Night and Evening windows still show
--    only average income efficiency due to congestion.
--
-- 6. Day part alone is not sufficient for decision-making.
--    Earnings vary widely within the same day part.
--
-- 7. Very high-earning windows are rare and time-specific.
--    Most earnings cluster in a stable mid-range.
--
-- 8. Day part improves understanding, but real value comes
--    only when combined with day, hour, and zone.
--
-- -----------------------------------------------------------


-- ===========================================================
-- TRIP LENGTH EFFICIENCY
-- (Income Impact of Short, Medium, and Long Trips)
-- ===========================================================
SELECT
    trip_length_type,
    pickup_weekday,
    pickup_hour,
    PUZone,

    -- Income efficiency
    ROUND(
        SUM(fare_amount) /
        NULLIF(SUM(trip_duration_min) / 60.0, 0),
        2
    ) AS earnings_per_hour,

    -- Demand normalized per day
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour
FROM analytics.taxi_trips_enriched
GROUP BY
    trip_length_type,
    pickup_weekday,
    pickup_hour,
    PUZone
HAVING
    COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date) >= 50
ORDER BY
    earnings_per_hour DESC;

-- -----------------------------------------------------------
-- KEY INSIGHTS
-- -----------------------------------------------------------
--
-- 1. Short trips can outperform long trips by a large margin,
--    but only in very specific zone–hour windows.
--
-- 2. Long trips dominate airport-related windows and provide
--    consistent earnings, especially late at night and early morning.
--
-- 3. Medium trips rarely lead in earnings and generally offer
--    no clear advantage over short or long trips.
--
-- 4. High earnings do not require high demand.
--    Some short-trip windows achieve top earnings with
--    moderate trip volume.
--
-- 5. Long trips offer income stability rather than high upside,
--    with earnings clustering in a predictable range.
--
-- 6. Short trips show wide earnings variation, meaning higher
--    upside but higher risk if conditions are not ideal.
--
-- 7. Trip length alone is not enough for decision-making.
--    Performance changes significantly by day, hour, and zone.
--
-- 8. Long trips favor stability, short trips favor efficiency,
--    and medium trips add limited strategic value.
--
-- -----------------------------------------------------------



-- ===========================================================
-- FINAL ANALYTICS DASHBOARD VIEW
-- (Filter-Ready Driver Performance Summary)
-- ===========================================================
-- This view combines all key metrics into a single table
-- that can be directly used in dashboards and reports.
--
-- Grain:
-- One row per Day × Hour × Pickup Zone × Context
--
-- Purpose:
-- • Compare zones across time
-- • Evaluate demand, earnings, and risk together
-- • Support driver positioning and income decisions
-- ===========================================================
CREATE OR ALTER VIEW analytics.driver_efficiency_dashboard AS
SELECT
    -- ===============================
    -- TIME DIMENSIONS
    -- ===============================
    pickup_weekday,
    DATENAME(
        WEEKDAY,
        DATEADD(DAY, pickup_weekday - 1, '2023-01-02')
    ) AS pickup_weekday_name,

    pickup_hour,
    PUZone,

    -- ===============================
    -- ZONE CLASSIFICATION
    -- ===============================
    CASE
        WHEN PUZone LIKE '%Airport%' THEN 'Airport'
        ELSE 'City'
    END AS zone_type,

    day_type,
    day_part,
    trip_length_type,

    -- ===============================
    -- SCALE & RELIABILITY METRICS
    -- ===============================
    COUNT(*) AS total_trips,
    COUNT(DISTINCT pickup_date) AS active_days,

    -- ===============================
    -- DEMAND (NORMALIZED)
    -- ===============================
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_trips_per_day_hour,

    -- ===============================
    -- TIME EFFICIENCY
    -- ===============================
    ROUND(
        AVG(trip_duration_min),
        2
    ) AS avg_trip_duration_min,

    -- ===============================
    -- BASE INCOME COMPONENTS (CRITICAL)
    -- ===============================
    SUM(fare_amount) AS total_fare,
    SUM(trip_duration_min) AS total_trip_duration_min,

    -- ===============================
    -- INCOME RISK
    -- ===============================
    ROUND(
        STDEV(trip_duration_min),
        2
    ) AS trip_duration_volatility_min

FROM analytics.taxi_trips_enriched
GROUP BY
    pickup_weekday,
    pickup_hour,
    PUZone,
    day_type,
    day_part,
    trip_length_type;



-- ===========================================================
-- FINAL DRIVER INCOME OPTIMIZATION SUGGESTIONS
-- ===========================================================
-- These suggestions are derived by combining:
-- demand, earnings per hour, congestion, and trip stability.
-- The goal is to convert demand into income efficiently,
-- not just increase the number of trips.


-- ===========================================================
-- 1. DO NOT CHASE DEMAND BLINDLY
-- ===========================================================

-- Zones:
-- • Central Park
-- • Penn Station / Madison Sq West
-- • Union Square
-- • East Chelsea

-- Data Observation:
-- • Very high avg_trips_per_day_hour
-- • Low earnings_per_hour
-- • High minutes_per_mile

-- Suggestion:
-- • Use these zones mainly for quick pickups
-- • Do not stay inside these zones for long durations
-- • Avoid chaining multiple short trips during peak congestion


-- ===========================================================
-- 2. USE PEAK HOURS MORE SMARTLY
-- ===========================================================
-- Zones:
-- • Times Square / Theatre District
-- • Penn Station / Madison Sq West
-- • Midtown Center
-- • East Chelsea

-- Data Observation:
-- • Peak hours show strong demand
-- • Earnings drop when drivers remain in congested zones

-- Suggestion:
-- • Enter these zones during peak hours
-- • Accept a trip quickly
-- • Exit the zone immediately after pickup

-- Benefit:
-- • Less time wasted in traffic
-- • More drivers get trips
-- • Higher earnings per hour


-- ===========================================================
-- 3. AIRPORT ZONES FOR MAXIMUM INCOME PER HOUR
-- ===========================================================
-- Zones:
-- • LaGuardia Airport

-- Data Observation:
-- • Consistently high earnings_per_hour
-- • Longer trip distances
-- • Lower congestion during off-peak hours

-- Suggestion:
-- • Target airport pickups during late night and early morning
-- • Prefer long trips over short urban trips


-- ===========================================================
-- 4. STABLE AND LOW-RISK INCOME ZONES
-- ===========================================================
-- Zones:
-- • Upper East Side
-- • Upper West Side
-- • East Village
-- • Midtown Center

-- Data Observation:
-- • Moderate earnings_per_hour
-- • Low trip_duration_volatility
-- • Consistent demand

-- Suggestion:
-- • Use these zones for predictable daily income
-- • Prefer weekday morning and early afternoon hours


-- ===========================================================
-- 5. SHORT-TRIP OPTIMIZATION (HIGH RISK / HIGH REWARD)
-- ===========================================================
-- Zones:
-- • Garment District
-- • West Village
-- • Midtown Center
-- • East Village

-- Data Observation:
-- • Extremely high earnings_per_hour in limited time windows
-- • High income variability outside those windows

-- Suggestion:
-- • Use these zones only when congestion is low
-- • Exit immediately once traffic increases
-- • Do not rely on these zones for all-day driving


-- ===========================================================
-- 6. MANAGE CONGESTION INSTEAD OF AVOIDING IT
-- ===========================================================
-- Zones:
-- • Times Square / Theatre District
-- • Penn Station / Madison Sq West
-- • East Chelsea
-- • Midtown Center

-- Data Observation:
-- • Congestion is consistent and unavoidable

-- Suggestion:
-- • Pre-position drivers before peak hours
-- • Use these zones mainly as pickup hubs
-- • Complete drop-offs in less congested zones

-- Benefit:
-- • Congestion time is shared across drivers
-- • Each driver loses less time on average

-- ===========================================================
-- FINAL EXECUTIVE TAKEAWAY
-- ===========================================================
-- • High demand does not guarantee high income
-- • Earnings improve when congestion exposure is reduced
-- • Airports maximize income per hour
-- • Residential zones provide stable income
-- • Peak-hour pickup rotation increases system efficiency

-- Best driver performance comes from balancing
-- demand, time efficiency, and income goals.

-- ===========================================================