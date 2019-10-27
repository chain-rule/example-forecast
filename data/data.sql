WITH
data AS (
  SELECT
    id,
    date,
    LAG(date) OVER (station_year) AS date_last,
    latitude,
    longitude,
    value / 10 AS temperature
  FROM
    `bigquery-public-data.ghcn_d.ghcnd_201*`
  INNER JOIN
    `bigquery-public-data.ghcn_d.ghcnd_stations` USING (id)
  WHERE
    CAST(_TABLE_SUFFIX AS INT64) BETWEEN 0 AND 9
    AND EXTRACT(MONTH FROM date) BETWEEN 6 AND 8
    AND element = 'TMIN'
    AND qflag IS NULL
  WINDOW
    station_year AS (
      PARTITION BY id, EXTRACT(YEAR FROM date)
      ORDER BY date
    )
)
SELECT
  id,
  MIN(date) AS date,
  latitude,
  longitude,
  ARRAY_AGG(DATE_DIFF(date, IFNULL(date_last, date), DAY) ORDER BY date) AS step,
  ARRAY_AGG(temperature ORDER BY date) AS temperature
FROM
  data
GROUP BY
  id, latitude, longitude, EXTRACT(YEAR FROM date)
ORDER BY
  id, date
