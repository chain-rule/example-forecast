WITH
-- Select relevant measurements
data_1 AS (
  SELECT
    id,
    date,
    -- Find the date of the previous observation
    LAG(date) OVER (station_year) AS date_last,
    latitude,
    longitude,
    -- Convert to degrees Celsius
    value / 10 AS temperature
  FROM
    `bigquery-public-data.ghcn_d.ghcnd_201*`
  INNER JOIN
    `bigquery-public-data.ghcn_d.ghcnd_stations` USING (id)
  WHERE
    -- Take years from 2010 to 2019
    CAST(_TABLE_SUFFIX AS INT64) BETWEEN 0 AND 9
    -- Take months from June to August
    AND EXTRACT(MONTH FROM date) BETWEEN 6 AND 8
    -- Take the maximum temperature
    AND element = 'TMAX'
    -- Take observations passed spatio-temporal quality-control checks
    AND qflag IS NULL
  WINDOW
    station_year AS (
      PARTITION BY id, EXTRACT(YEAR FROM date)
      ORDER BY date
    )
),
-- Group into complete examples (a specific station and a specific year)
data_2 AS (
  SELECT
    id,
    MIN(date) AS date,
    latitude,
    longitude,
    -- Compute gaps between observations
    ARRAY_AGG(DATE_DIFF(date, IFNULL(date_last, date), DAY) ORDER BY date) AS duration,
    ARRAY_AGG(temperature ORDER BY date) AS temperature
  FROM
    data_1
  GROUP BY
    id, latitude, longitude, EXTRACT(YEAR FROM date)
)
-- Partition into training, validation, and testing sets
SELECT
  *,
  CASE
    WHEN EXTRACT(YEAR FROM date) < 2019 THEN 'analysis,training'
    WHEN MOD(ABS(FARM_FINGERPRINT(id)), 100) < 50 THEN 'validation'
    ELSE 'testing'
  END AS mode
FROM
  data_2
