/* Schema for Spotify Data DB
 * --------------------------
 * 
 * This script creates all needed tables for storing my Spotify Data 
 * in a normalized and convenient way.
 * 
 * Notes:
 * -	Due to an early version bug, SQLite does not enforce NOT NULL on 
 * non-Integer PRIMARY KEY columns in non-STRICT tables. 
 * For this reason, tables are defined as STRICT, and/or their columns 
 * are explicitly defined as NOT NULL. 
 * -	Integer primary key columns are considered an alias for ROWID. 
 * Those columns' value auto-increments upon INSERT, unless manually specified.
 * 
 * More info here: 
 * https://www.sqlite.org/lang_createtable.html#the_primary_key
 * https://www.sqlite.org/stricttables.html
 * 
 */

-- DB Tables definition --

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

CREATE TABLE IF NOT EXISTS albums (
	album_id TEXT PRIMARY KEY NOT NULL,
	href TEXT,
	uri TEXT,
	name TEXT,
	album_type TEXT,
	total_tracks INTEGER,
	release_date TEXT,
	release_date_precision TEXT
);

CREATE TABLE IF NOT EXISTS artists (
	artist_id TEXT PRIMARY KEY NOT NULL,
	href TEXT,
	uri TEXT,
	name TEXT,
	total_followers INTEGER,
	popularity INTEGER
);

CREATE TABLE IF NOT EXISTS artists_albums (
	artist_id TEXT NOT NULL,
	album_id TEXT NOT NULL,
	PRIMARY KEY (artist_id, album_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS albums_tracks (
	album_id TEXT NOT NULL,
	track_id TEXT NOT NULL,
	PRIMARY KEY (album_id, track_id),
	FOREIGN KEY (album_id) REFERENCES albums(album_id),
	FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);

CREATE TABLE IF NOT EXISTS genres (
	genre_id INTEGER PRIMARY KEY NOT NULL,
	name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS artists_genres (
	artist_id TEXT NOT NULL,
	genre_id TEXT NOT NULL,
	PRIMARY KEY (artist_id, genre_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS linked_tracks (
	linked_from_id TEXT NOT NULL,
	track_known_id TEXT NOT NULL,
	PRIMARY KEY (linked_from_id),
	FOREIGN KEY (track_known_id) REFERENCES tracks(track_id) 
);

CREATE TABLE IF NOT EXISTS tracks_listen_history (
	time_stamp TEXT NOT NULL,
	username TEXT NOT NULL,
	track_id TEXT NOT NULL,
	platform TEXT,
	ms_played INTEGER,
	conn_country TEXT,
	uri TEXT,
	reason_start TEXT,
	reason_end TEXT,
	shuffle BOOLEAN,
	offline BOOLEAN,
	incognito_mode BOOLEAN,
	skipped TEXT,
	PRIMARY KEY (time_stamp, username, track_id)
	FOREIGN KEY (track_id) REFERENCES linked_tracks(linked_from_id)
);


-- Indexes definition

CREATE INDEX IF NOT EXISTS idx_tracks_name
	ON tracks (name);

CREATE INDEX IF NOT EXISTS idx_albums_name
	ON albums (name);

CREATE INDEX IF NOT EXISTS idx_albums_release_date
	ON albums (release_date, release_date_precision);

CREATE INDEX IF NOT EXISTS idx_genres_name
	ON genres(name);

CREATE INDEX IF NOT EXISTS idx_tracks_listen_history_platform
	ON tracks_listen_history (platform);

CREATE INDEX IF NOT EXISTS idx_tracks_listen_history_conn_country
	ON tracks_listen_history (conn_country);

CREATE INDEX IF NOT EXISTS idx_tracks_listen_history_reason
	ON tracks_listen_history (reason_start, reason_end);
	

-- Views definition

CREATE VIEW IF NOT EXISTS v_tracks_listen_history 
	AS SELECT time_stamp,
			username,
			track_id AS track_listened_id,
			linked_tracks.track_known_id AS track_known_id,
			platform,
			ms_played,
			conn_country,
			uri,
			reason_start,
			reason_end,
			shuffle,
			offline,
			incognito_mode,
			skipped
	FROM tracks_listen_history
	INNER JOIN linked_tracks ON linked_tracks.linked_from_id = tracks_listen_history.track_id;


