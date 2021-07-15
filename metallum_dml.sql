USE metallum;


SELECT
  genre,
  COUNT(*) AS band_count
FROM
  bands
GROUP BY
  genre
ORDER BY
  band_count DESC;


SELECT
  *
FROM
  albums_vw AS a
ORDER BY
  a.band_index
LIMIT
  1000;


SELECT
  c.country_name,
  g.genre_name,
  COUNT(*) AS band_count
FROM
  countries AS c
  INNER JOIN band_countries_vw AS bc ON c.country_id = bc.country_id
  INNER JOIN band_genres AS g ON bc.band_id = g.band_id
GROUP BY
  c.country_name,
  g.genre_name;


SELECT
  b.*,
  g.changed_genre
FROM
  bands AS b
  INNER JOIN (
    SELECT
      bg.band_id,
      IF(MIN(bg.phase_name) IS NULL, 0, 1) AS changed_genre
    FROM
      band_genres AS bg
    GROUP BY
      bg.band_id
  ) AS g ON b.band_id = g.band_id
LIMIT
  100;

-- subqueries
SELECT
  s.band_id,
  d.genre_name AS core_genre,
  s.early,
  s.mid,
  s.later
FROM
  (
    SELECT
      bg.band_id,
      IF(bg.phase_name = 'early', bg.genre_name, NULL) AS early,
      IF(bg.phase_name = 'mid', bg.genre_name, NULL) AS mid,
      IF(bg.phase_name = 'later', bg.genre_name, NULL) AS later
    FROM
      band_genres AS bg
  ) AS s
  LEFT JOIN (
    SELECT
      bg.band_id,
      bg.genre_name
    FROM
      band_genres AS bg
    WHERE
      phase_name IS NULL
  ) AS d ON s.band_id = d.band_id
WHERE
  (
    s.early IS NOT NULL
    OR s.mid IS NOT NULL
    OR s.later IS NOT NULL
  )
  AND s.band_id = 112919
ORDER BY
  s.band_id;


SELECT
  *
FROM
  bands AS b
WHERE
  b.band_id = 28570;

