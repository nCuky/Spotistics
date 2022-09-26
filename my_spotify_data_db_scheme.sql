CREATE TABLE IF NOT EXISTS tracks (
	track_id TEXT PRIMARY KEY NOT NULL,
	href TEXT,
	uri TEXT,
	disc_number INTEGER,
	duration_ms INTEGER,
	explicit BOOLEAN,
	name TEXT,
	preview_url TEXT,
	track_number INTEGER,
	is_local BOOLEAN,
	popularity INTEGER,
	is_playable BOOLEAN
);

CREATE INDEX idx_tracks_name
	ON tracks (name);

CREATE TABLE IF NOT EXISTS albums (
	album_id TEXT PRIMARY KEY NOT NULL,
	href TEXT,
	uri TEXT,
	name TEXT,
	album_type TEXT,
	total_tracks INTEGER,
	release_date TEXT,
	release_date_precision TEXT
--       fk_track_id TEXT,
--       FOREIGN KEY (fk_track_id) REFERENCES tracks(pk_id),
--       	PRIMARY KEY (pk_id)
--       , fk_track_id)
);

CREATE INDEX idx_albums_name
	ON albums (name);

CREATE INDEX idx_albums_release_date
	ON albums (release_date, release_date_precision);


CREATE TABLE IF NOT EXISTS artists (
	artist_id TEXT PRIMARY KEY NOT NULL,
	href TEXT,
	uri TEXT,
	name TEXT,
	total_followers INTEGER,
	popularity INTEGER
);

CREATE TABLE IF NOT EXISTS artists_albums (
	artist_id TEXT PRIMARY KEY NOT NULL,
	album_id TEXT PRIMARY KEY NOT NULL,
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS albums_tracks (
	album_id TEXT PRIMARY KEY NOT NULL,
	track_id TEXT PRIMARY KEY NOT NULL,
	FOREIGN KEY (album_id) REFERENCES albums(album_id),
	FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);

CREATE TABLE IF NOT EXISTS genres (
	genre_id TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS artists_genres (
	artist_id TEXT PRIMARY KEY NOT NULL,
	genre_id TEXT PRIMARY KEY NOT NULL,
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
)

--CREATE TABLE IF NOT EXISTS artists_albums (
--       pk_id TEXT,
--       href TEXT,
--       uri TEXT,
--       name TEXT,
--       fk_album_id TEXT,
--       FOREIGN KEY (fk_album_id) REFERENCES albums(pk_id),
--       FOREIGN KEY (pk_id) REFERENCES artists_tracks(pk_id),
--       PRIMARY KEY (pk_id, fk_album_id)
--);
--
--CREATE INDEX idx_artists_albums_name
--ON artists_albums (name);


--CREATE TABLE IF NOT EXISTS artists_tracks (
--       pk_id TEXT,
--       href TEXT,
--       uri TEXT,
--       name TEXT,
--       fk_track_id TEXT,
--       FOREIGN KEY (fk_track_id) REFERENCES tracks(pk_id),
--       FOREIGN KEY (pk_id) REFERENCES artists_albums(pk_id),
--       PRIMARY KEY (pk_id, fk_track_id)
--);
--
--CREATE INDEX idx_artists_tracks_name
--ON artists_tracks (name);


CREATE TABLE IF NOT EXISTS tracks_linked_from (
	track_id TEXT PRIMARY KEY NOT NULL,
	linked_from_id TEXT,
	FOREIGN KEY (track_id) REFERENCES tracks(track_id) 
);

CREATE TABLE IF NOT EXISTS tracks_listen_history (
	time_stamp TEXT PRIMARY KEY NOT NULL,
	username TEXT PRIMARY KEY NOT NULL,
	track_id TEXT PRIMARY KEY NOT NULL,
	platform TEXT,
	ms_played integer,
	conn_country TEXT,
	uri TEXT,
	reason_start TEXT,
	reason_end TEXT,
	shuffle BOOLEAN,
	offline BOOLEAN,
	incognito_mode BOOLEAN,
	skipped TEXT,
	FOREIGN KEY (track_id) REFERENCES tracks_linked_from(track_id)
);

CREATE INDEX idx_tracks_listen_history_platform
	ON tracks_listen_history (platform);

CREATE INDEX idx_tracks_listen_history_conn_country
	ON tracks_listen_history (conn_country);

CREATE INDEX idx_tracks_listen_history_reason
	ON tracks_listen_history (reason_start, reason_end);