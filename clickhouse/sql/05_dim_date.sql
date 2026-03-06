-- Date dimension (generated, not CDC-sourced).
-- Enables calendar-based analytics: weekday/weekend splits, monthly trends, quarterly rollups.

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key     Date,
    year         UInt16,
    quarter      UInt8,
    month        UInt8,
    month_name   String,
    week_of_year UInt8,
    day_of_month UInt8,
    day_of_week  UInt8,
    day_name     String,
    is_weekend   UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY date_key;

-- Populate 5 years of dates (2024-01-01 through ~2028-12-30)
INSERT INTO dwh.dim_date
SELECT
    d AS date_key,
    toYear(d) AS year,
    toQuarter(d) AS quarter,
    toMonth(d) AS month,
    dateName('month', d) AS month_name,
    toISOWeek(d) AS week_of_year,
    toDayOfMonth(d) AS day_of_month,
    toDayOfWeek(d) AS day_of_week,
    dateName('day', d) AS day_name,
    if(toDayOfWeek(d) IN (6, 7), 1, 0) AS is_weekend
FROM (
    SELECT toDate('2024-01-01') + number AS d
    FROM numbers(365 * 5)
);
