USE metallum;

ALTER TABLE metallum.albums
ADD UNIQUE INDEX dk_albums (metallum_album_id, metallum_band_id);

ALTER TABLE metallum.albums
ADD UNIQUE INDEX ix_albums_album_id (album_id);

ALTER TABLE metallum.band_genres
ADD UNIQUE INDEX dk_band_genres (band_id, genre_name (40), phase_name (5));

ALTER TABLE metallum.band_genres
ADD UNIQUE INDEX ix_band_genres_band_genre_id (band_genre_id);

ALTER TABLE metallum.bands
ADD UNIQUE INDEX dk_bands (metallum_band_id);

ALTER TABLE metallum.bands
ADD UNIQUE INDEX ix_bands_band_id (band_id);

ALTER TABLE metallum.countries
ADD UNIQUE INDEX dk_countries (country_name (50));

ALTER TABLE metallum.countries
ADD UNIQUE INDEX ix_countries_country_id (country_id);

ALTER TABLE metallum.tracks
ADD UNIQUE INDEX ix_tracks_track_id (track_id);

ALTER TABLE metallum.tracks
ADD INDEX nix_tracks_band_album (metallum_band_id, metallum_album_id);

ALTER TABLE metallum.genres
ADD UNIQUE INDEX dk_genres (genre_name (50));

ALTER TABLE metallum.genres
ADD UNIQUE INDEX ix_genres_genre_id (genre_id);