SELECT
  id,
  latitude,
  longitude,
  ARRAY_AGG(date ORDER BY date) AS date,
  ARRAY_AGG(value / 10 ORDER BY date) AS temperature
FROM
  `bigquery-public-data.ghcn_d.ghcnd_*`
INNER JOIN
  `bigquery-public-data.ghcn_d.ghcnd_stations` USING (id)
WHERE
  REGEXP_CONTAINS(_TABLE_SUFFIX, '201[0-8]')
  AND EXTRACT(MONTH FROM date) BETWEEN 6 AND 8
  AND element = 'TMIN'
  AND qflag IS NULL
GROUP BY
  1, 2, 3
