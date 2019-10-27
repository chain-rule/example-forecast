WITH
data_1 AS (
  SELECT
    id,
    date,
    -- The data of the previous observation for the station and year
    LAG(date) OVER (station_year) AS date_last,
    latitude,
    longitude,
    -- Convert from tenths of a unit to units
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
    -- Take the average temperature
    AND element = 'TAVG'
    -- Take observations passed spatio-temporal quality-control checks
    AND qflag IS NULL
  WINDOW
    station_year AS (
      PARTITION BY id, EXTRACT(YEAR FROM date)
      ORDER BY date
    )
),
data_2 AS (
  SELECT
    id,
    MIN(date) AS date,
    latitude,
    longitude,
    -- Compute gaps between observatios
    ARRAY_AGG(DATE_DIFF(date, IFNULL(date_last, date), DAY) ORDER BY date) AS duration,
    ARRAY_AGG(temperature ORDER BY date) AS temperature
  FROM
    data_1
  GROUP BY
    -- Group to have have one record per station and year
    id, latitude, longitude, EXTRACT(YEAR FROM date)
)
SELECT
  *,
  CASE
    WHEN EXTRACT(YEAR FROM date) < 2019 THEN 'analysis,training'
    WHEN MOD(ABS(FARM_FINGERPRINT(id)), 100) < 50 THEN 'validation'
    ELSE 'testing'
  END AS mode
FROM
  data_2
