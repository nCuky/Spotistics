CREATE TABLE IF NOT EXISTS tracks (
       pk_id text primary key,
       href text,
       uri text,
       disc_number integer,
       duration_ms integer,
       explicit boolean,
       name text,
       preview_url text,
       track_number integer,
       is_local boolean,
       popularity integer,
       is_playable boolean
);

CREATE INDEX idx_tracks_name
ON tracks (name);


CREATE TABLE IF NOT EXISTS albums (
       pk_id text,
       href text,
       uri text,
       name text,
       album_type text,
       total_tracks integer,
       release_date text,
       release_date_precision text,
--       fk_track_id text,
--       foreign key (fk_track_id) references tracks(pk_id),
       primary key (pk_id)
--       , fk_track_id)
);

CREATE INDEX idx_albums_name
ON albums (name);

CREATE INDEX idx_albums_release_date
ON albums (release_date, release_date_precision);


CREATE TABLE IF NOT EXISTS artists (
       pk_id text,
       href text,
       uri text,
       name text,
       total_followers integer,
       popularity integer,
       primary key (pk_id)
);

CREATE TABLE IF NOT EXISTS albums_of_artists (
       pk_artist_id text,
       pk_album_id text,
       primary key (pk_artist_id, pk_album_id),
       foreign key (pk_artist_id) references artists(pk_id),
       foreign key (pk_album_id) references albums(pk_id)
);

CREATE TABLE IF NOT EXISTS tracks_of_albums (
       pk_album_id text,
       pk_track_id text,
       primary key (pk_album_id, fk_track_id),
       foreign key (pk_album_id) references albums(pk_id),
       foreign key (fk_track_id) references tracks(pk_id)
);

CREATE TABLE IF NOT EXISTS genres (
)      pk_id text
       primary key (pk_id)

CREATE TABLE IF NOT EXISTS genres_of_artists (
       pk_artist_id text,
       pk_genre_id text,
       primary key (pk_artist_id, pk_genre_id),
       foreign key (pk_artist_id) references artists(pk_id),
       foreign key (pk_genre_id) references genres(pk_id)

CREATE TABLE IF NOT EXISTS artists_albums (
       pk_id text,
       href text,
       uri text,
       name text,
       fk_album_id text,
       foreign key (fk_album_id) references albums(pk_id),
       foreign key (pk_id) references artists_tracks(pk_id),
       primary key (pk_id, fk_album_id)
);

CREATE INDEX idx_artists_albums_name
ON artists_albums (name);


CREATE TABLE IF NOT EXISTS artists_tracks (
       pk_id text,
       href text,
       uri text,
       name text,
       fk_track_id text,
       foreign key (fk_track_id) references tracks(pk_id),
       foreign key (pk_id) references artists_albums(pk_id),
       primary key (pk_id, fk_track_id)
);

CREATE INDEX idx_artists_tracks_name
ON artists_tracks (name);


CREATE TABLE IF NOT EXISTS tracks_linked_from (
       pk_id text primary key,
--       href text,
--       uri text,
       fk_track_id text,
       foreign key (fk_track_id) references tracks(pk_id)
);

CREATE TABLE IF NOT EXISTS tracks_listen_history (
       pk_timestamp text,
       pk_username text,
       pk_track_id text,
       platform text,
       ms_played integer,
       conn_country text,
       uri text,
       reason_start text,
       reason_end text,
       shuffle boolean,
       offline boolean,
       incognito_mode boolean,
       skipped text,
       foreign key (pk_track_id) references tracks_linked_from(pk_id),
       primary key (pk_timestamp, pk_username, pk_track_id)
);

CREATE INDEX idx_tracks_listen_history_platform
ON tracks_listen_history (platform);

CREATE INDEX idx_tracks_listen_history_conn_country
ON tracks_listen_history (conn_country);

CREATE INDEX idx_tracks_listen_history_reason
ON tracks_listen_history (reason_start, reason_end);