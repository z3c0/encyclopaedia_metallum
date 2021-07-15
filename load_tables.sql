USE metallum;

TRUNCATE TABLE band_genres;

DELETE FROM bands;

DELETE FROM countries;

--countries
START TRANSACTION;

INSERT INTO countries (country_name)
SELECT sc.country_name FROM stg_countries AS sc
LEFT JOIN countries AS c
    ON sc.country_name = c.country_name
WHERE c.country_name IS NULL;


UPDATE countries SET countries.deleted = 1
WHERE countries.country_name NOT IN (SELECT country_name FROM stg_countries);

COMMIT;

--bands
START TRANSACTION;

INSERT INTO bands (metallum_band_id, band_name, genre, country_id, band_status, band_url, deleted)
SELECT sb.metallum_band_id, sb.band_name, sb.genre, c.country_id, sb.band_status, sb.band_url, 0 
FROM stg_bands AS sb
LEFT JOIN countries AS c
    ON sb.country = c.country_name
LEFT JOIN bands AS b
    ON sb.metallum_band_id = b.metallum_band_id
    AND sb.band_name = b.band_name
WHERE b.band_id IS NULL;

UPDATE bands, stg_bands, countries
SET 
    bands.genre = stg_bands.genre, bands.country_id = countries.country_id,
    bands.band_status = stg_bands.band_status, bands.band_url = stg_bands.band_url
WHERE 
    stg_bands.country = countries.country_name
    AND stg_bands.metallum_band_id = bands.metallum_band_id
    AND stg_bands.band_name = bands.band_name;


COMMIT;

--band genres
START TRANSACTION;

INSERT INTO band_genres (band_id, genre_name, phase_name)
SELECT b.band_id, bg.genre_name, bg.phase_name 
FROM band_genres_vw AS bg
INNER JOIN stg_bands AS sb
    ON bg.stg_band_id = sb.stg_band_id
INNER JOIN bands AS b
    ON sb.metallum_band_id = b.metallum_band_id
    AND sb.band_name = b.band_name;

COMMIT;

