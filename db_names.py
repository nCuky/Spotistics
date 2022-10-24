from dataclasses import dataclass


@dataclass(frozen = True)
class TRACKS:
    TBL_NAME = 'tracks'

    ID = 'track_id'
    NAME = 'name'
    DURATION_MS = 'duration_ms'
    DISC_NUMBER = 'disc_number'
    TRACK_NUMBER = 'track_number'
    EXPLICIT = 'explicit'
    POPULARITY = 'popularity'
    IS_LOCAL = 'is_local'
    IS_PLAYABLE = 'is_playable'
    ISRC = 'isrc'
    HREF = 'href'
    URI = 'uri'
    PREVIEW_URL = 'preview_url'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class ALBUMS:
    TBL_NAME = 'albums'

    ID = 'album_id'
    NAME = 'name'
    TOTAL_TRACKS = 'total_tracks'
    RELEASE_DATE = 'release_date'
    RELEASE_DATE_PRECISION = 'release_date_precision'
    ALBUM_TYPE = 'album_type'
    IS_AVAILABLE = 'is_available'
    HREF = 'href'
    URI = 'uri'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class ARTISTS:
    TBL_NAME = 'artists'

    ID = 'artist_id'
    NAME = 'name'
    TOTAL_FOLLOWERS = 'total_followers'
    POPULARITY = 'popularity'
    HREF = 'href'
    URI = 'uri'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class ARTISTS_ALBUMS:
    TBL_NAME = 'artists_albums'

    ARTIST_ID = 'artist_id'
    ALBUM_ID = 'album_id'
    ALBUM_GROUP = 'album_group'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class ALBUMS_TRACKS:
    TBL_NAME = 'albums_tracks'

    ALBUM_ID = 'album_id'
    TRACK_ID = 'track_id'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class GENRES:
    TBL_NAME = 'genres'

    ID = 'genre_id'
    NAME = 'name'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class ARTISTS_GENRES:
    TBL_NAME = 'artists_genres'

    ARTIST_ID = 'artist_id'
    GENRE_ID = 'genre_id'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class LINKED_TRACKS:
    TBL_NAME = 'linked_tracks'

    FROM_ID = 'linked_from_id'
    RELINKED_ID = 'track_known_id'
    IS_LINKED = 'is_linked'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class LINKED_ALBUMS:
    TBL_NAME = 'linked_albums'

    FROM_ID = 'linked_from_id'
    RELINKED_ID = 'album_known_id'
    IS_LINKED = 'is_linked'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class TRACKS_LISTEN_HISTORY:
    TBL_NAME = 'tracks_listen_history'

    USERNAME = 'username'
    TIMESTAMP = 'time_stamp'
    TRACK_ID = 'track_id'
    MS_PLAYED = 'ms_played'
    REASON_START = 'reason_start'
    REASON_END = 'reason_end'
    SKIPPED = 'skipped'
    PLATFORM = 'platform'
    CONN_COUNTRY = 'conn_country'
    URI = 'uri'
    SHUFFLE = 'shuffle'
    OFFLINE = 'offline'
    INCOGNITO_MODE = 'incognito_mode'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


@dataclass(frozen = True)
class V_KNOWN_LISTEN_HISTORY:
    VIEW_NAME = 'v_known_listen_history'

    USERNAME = TRACKS_LISTEN_HISTORY.USERNAME
    TIMESTAMP = TRACKS_LISTEN_HISTORY.TIMESTAMP
    TRACK_LISTENED_ID = 'track_listened_id'
    TRACK_KNOWN_ID = LINKED_TRACKS.RELINKED_ID
    TRACK_NAME = 'track_name'
    ALBUM_KNOWN_ID = 'album_known_id'
    ALBUM_NAME = 'album_name'
    ALBUM_ARTIST_ID = 'album_artist_id'
    ALBUM_ARTIST_NAME = 'album_artist_name'
    MS_PLAYED = TRACKS_LISTEN_HISTORY.MS_PLAYED
    TRACK_DURATION_MS = 'track_duration_ms'
    REASON_START = TRACKS_LISTEN_HISTORY.REASON_START
    REASON_END = TRACKS_LISTEN_HISTORY.REASON_END
    SKIPPED = TRACKS_LISTEN_HISTORY.SKIPPED
    PLATFORM = TRACKS_LISTEN_HISTORY.PLATFORM
    CONN_COUNTRY = TRACKS_LISTEN_HISTORY.CONN_COUNTRY
    URI = TRACKS_LISTEN_HISTORY.URI
    SHUFFLE = TRACKS_LISTEN_HISTORY.SHUFFLE
    OFFLINE = TRACKS_LISTEN_HISTORY.OFFLINE
    INCOGNITO_MODE = TRACKS_LISTEN_HISTORY.INCOGNITO_MODE
    CREATED_AT = TRACKS_LISTEN_HISTORY.CREATED_AT
    UPDATED_AT = TRACKS_LISTEN_HISTORY.UPDATED_AT
