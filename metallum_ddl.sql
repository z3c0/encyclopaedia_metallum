 USE metallum;

CREATE VIEW albums_vw AS
SELECT  b.band_id 
       ,a.album_id 
       ,a.album_name 
       ,a.album_type 
       ,a.year 
       ,a.review 
       ,a.album_url
FROM albums AS a
INNER JOIN bands AS b
ON a.band_name = b.band_name AND a.metallum_band_id = b.metallum_band_id;

CREATE VIEW band_countries_vw AS
SELECT  c.country_id 
       ,b.band_id
FROM countries AS c
INNER JOIN bands AS b
ON c.country_name = b.country;