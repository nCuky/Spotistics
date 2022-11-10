/* Schema for Spotify Data DB
 * --------------------------
 * This script creates all needed tables for storing my Spotify Data 
 * in a normalized and convenient way.
 * It also defines additional indexes, views, triggers, etc., required
 * for the DB.
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
 */

-- DB Tables definition --

CREATE TABLE IF NOT EXISTS tracks (
	track_id TEXT PRIMARY KEY NOT NULL,
	name TEXT,
	duration_ms INTEGER,
	disc_number INTEGER,
	track_number INTEGER,
	explicit BOOLEAN,
	popularity INTEGER,
	is_local BOOLEAN,
	is_playable BOOLEAN,
	isrc TEXT,
	href TEXT,
	uri TEXT,
	preview_url TEXT,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME 
);

CREATE TABLE IF NOT EXISTS albums (
	album_id TEXT PRIMARY KEY NOT NULL,
	name TEXT,
	total_tracks INTEGER,
	release_date TEXT,
	release_date_precision TEXT,
	album_type TEXT,
	is_available BOOLEAN,
	href TEXT,
	uri TEXT,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS artists (
	artist_id TEXT PRIMARY KEY NOT NULL,
	name TEXT,
	total_followers INTEGER,
	popularity INTEGER,
	href TEXT,
	uri TEXT,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS genres (
	genre_id INTEGER PRIMARY KEY NOT NULL,
	name TEXT NOT NULL UNIQUE,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS artists_albums (
	artist_id TEXT NOT NULL,
	album_id TEXT NOT NULL,
	album_group TEXT,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME,
	PRIMARY KEY (artist_id, album_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS albums_tracks (
	album_id TEXT NOT NULL,
	track_id TEXT NOT NULL,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME,
	PRIMARY KEY (album_id, track_id),
	FOREIGN KEY (album_id) REFERENCES albums(album_id),
	FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);

CREATE TABLE IF NOT EXISTS artists_genres (
	artist_id TEXT NOT NULL,
	genre_id TEXT NOT NULL,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME,
	PRIMARY KEY (artist_id, genre_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS linked_tracks (
	linked_from_id TEXT NOT NULL,
	track_known_id TEXT NOT NULL,
	is_linked BOOLEAN GENERATED ALWAYS AS (linked_from_id != track_known_id) VIRTUAL,
	PRIMARY KEY (linked_from_id),
	FOREIGN KEY (track_known_id) REFERENCES tracks(track_id) 
);

CREATE TABLE IF NOT EXISTS linked_albums (
	linked_from_id TEXT NOT NULL,
	album_known_id TEXT NOT NULL,
	is_linked BOOLEAN GENERATED ALWAYS AS (linked_from_id != album_known_id) VIRTUAL,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME,
	PRIMARY KEY (linked_from_id),
	FOREIGN KEY (album_known_id) REFERENCES albums(album_id) 
);

CREATE TABLE IF NOT EXISTS tracks_listen_history (
	username TEXT NOT NULL,
	time_stamp TEXT NOT NULL,
	track_id TEXT NOT NULL,
	ms_played INTEGER,
	reason_start TEXT,
	reason_end TEXT,
	skipped TEXT,
	platform TEXT,
	conn_country TEXT,
	uri TEXT,
	shuffle BOOLEAN,
	offline BOOLEAN,
	incognito_mode BOOLEAN,
	created_at DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
	updated_at DATETIME,
	PRIMARY KEY (username, time_stamp, track_id)
	FOREIGN KEY (track_id) REFERENCES linked_tracks(linked_from_id)
);


-- Triggers definition --

CREATE TRIGGER IF NOT EXISTS trg_update_tracks_updated_at
	AFTER UPDATE ON tracks
	BEGIN 
		UPDATE tracks 
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE track_id = NEW.track_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_albums_updated_at
	AFTER UPDATE ON albums
	BEGIN 
		UPDATE albums
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE album_id = NEW.album_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_artists_updated_at
	AFTER UPDATE ON artists
	BEGIN 
		UPDATE albums
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE artist_id = NEW.artist_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_genres_updated_at
	AFTER UPDATE ON genres
	BEGIN 
		UPDATE genres
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE genre_id = NEW.genre_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_artists_albums_updated_at
	AFTER UPDATE ON artists_albums
	BEGIN 
		UPDATE artists_albums
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE artist_id = NEW.artist_id
			  AND album_id  = NEW.album_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_albums_tracks_updated_at
	AFTER UPDATE ON albums_tracks
	BEGIN 
		UPDATE albums_tracks
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE album_id = NEW.album_id
			  AND track_id = NEW.track_id;
	END;

CREATE TRIGGER IF NOT EXISTS trg_update_tracks_listen_history_updated_at
	AFTER UPDATE ON tracks_listen_history
	BEGIN 
		UPDATE tracks_listen_history
			SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime'))
			WHERE track_id = NEW.track_id;
	END;


-- Indexes definition --

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
	

-- Views definition --

CREATE VIEW IF NOT EXISTS v_tracks
	AS SELECT tracks.track_id,
			tracks.name,
			tracks.duration_ms,
			tracks.disc_number,
			tracks.track_number,
			tracks.explicit,
			tracks.popularity,
			tracks.is_local,
			tracks.is_playable,
			tracks.isrc,
			tracks.href,
			tracks.uri,
			tracks.preview_url,
			linked_tracks.is_linked AS is_linked,
			linked_tracks.linked_from_id AS linked_from_id,
			tracks.created_at,
			tracks.updated_at
	FROM tracks
	LEFT OUTER JOIN linked_tracks ON linked_tracks.track_known_id = tracks.track_id
								  --AND linked_tracks.is_linked = TRUE
	ORDER BY name ASC;

CREATE VIEW IF NOT EXISTS v_artists_albums
	AS SELECT artists_albums.artist_id,
			  artists.name AS artist_name,
--			  artists_albums.album_group,
			  artists_albums.album_id,
			  albums.name AS album_name,
			  albums.is_available,
			  artists_albums.created_at,
			  artists_albums.updated_at
	FROM artists_albums
	INNER JOIN artists ON artists.artist_id = artists_albums.artist_id 
	INNER JOIN albums ON albums.album_id = artists_albums.album_id
	ORDER BY artist_name ASC, 
			 artists_albums.album_group ASC, 
			 album_name ASC;
								 
CREATE VIEW IF NOT EXISTS v_albums_tracks
	AS SELECT albums_tracks.album_id,
			  albums_tracks.track_id,
			  linked_albums.is_linked AS is_album_linked,
			  linked_albums.album_known_id AS album_known_id, 
			  linked_tracks.is_linked AS is_track_linked,
			  linked_tracks.track_known_id AS track_known_id
	FROM albums_tracks
	INNER JOIN linked_albums ON linked_albums.linked_from_id = albums_tracks.album_id
	INNER JOIN linked_tracks ON linked_tracks.linked_from_id = albums_tracks.track_id;

CREATE VIEW IF NOT EXISTS v_linked_tracks
	AS SELECT 	linked_tracks.linked_from_id,
				linked_tracks.track_known_id,
				linked_tracks.is_linked,
				tracks.name,
				tracks.duration_ms,
				tracks.disc_number,
				tracks.track_number
	FROM linked_tracks
	LEFT OUTER JOIN tracks ON tracks.track_id = linked_tracks.track_known_id 
	ORDER BY name ASC,
			 track_known_id ASC;

CREATE VIEW IF NOT EXISTS v_linked_albums
	AS SELECT 	linked_albums.linked_from_id,
				linked_albums.album_known_id,
				linked_albums.is_linked,
				albums.name
	FROM linked_albums
	LEFT OUTER JOIN albums ON albums.album_id = linked_albums.album_known_id  
	ORDER BY name ASC,
			 album_known_id ASC;

CREATE VIEW IF NOT EXISTS v_known_listen_history 
	AS SELECT tracks_listen_history.username,
			  tracks_listen_history.time_stamp,			  
			  tracks_listen_history.track_id AS track_listened_id,
			  linked_tracks.track_known_id AS track_known_id,
			  tracks.name AS track_name,
			  albums.album_id AS album_known_id,
			  albums.name AS album_name,
			  artists.artist_id AS album_artist_id,
			  artists.name AS album_artist_name,
			  tracks_listen_history.ms_played,
			  tracks.duration_ms AS track_duration_ms,
			  tracks_listen_history.reason_start,
			  tracks_listen_history.reason_end,
			  tracks_listen_history.skipped,
			  tracks_listen_history.platform,
			  tracks_listen_history.conn_country,
		      tracks_listen_history.uri,
			  tracks_listen_history.shuffle,
			  tracks_listen_history.offline,
			  tracks_listen_history.incognito_mode,
			  tracks_listen_history.created_at,
			  tracks_listen_history.updated_at 
	FROM tracks_listen_history
	INNER JOIN linked_tracks ON linked_tracks.linked_from_id = tracks_listen_history.track_id
	INNER JOIN tracks ON tracks.track_id = linked_tracks.track_known_id
	INNER JOIN albums_tracks ON albums_tracks.track_id = linked_tracks.track_known_id 
	INNER JOIN albums ON albums.album_id = albums_tracks.album_id 
	INNER JOIN artists_albums ON artists_albums.album_id = albums_tracks.album_id
	INNER JOIN artists ON artists.artist_id = artists_albums.artist_id
	ORDER BY username ASC,
             time_stamp ASC,
             track_known_id ASC,
             album_artist_name ASC;		
			