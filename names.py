from dataclasses import dataclass


@dataclass(frozen = True)
class Spdt:
    TIMESTAMP = 'time_stamp'
    MS_PLAYED = 'ms_played'
    ALBUM_ARTIST_NAME = 'album_artist_name'
    ALBUM_NAME = 'album_name'
    TRACK_NAME = 'track_name'
    TRACK_ID = 'track_id'
    TRACK_KNOWN_ID = 'track_known_id'
    TRACK_URI = 'uri'
    EPISODE_NAME = 'episode_name'
    EPISODE_SHOW_NAME = 'episode_show_name'
    EPISODE_URI = 'spotify_episode_uri'

    # Technical metadata
    USERNAME = 'username'
    CONN_COUNTRY = 'conn_country'
    IP_ADDRESS = 'ip_addr_decrypted'
    USER_AGENT = 'user_agent_decrypted'
    PLATFORM = 'platform'
    INCOGNITO = 'incognito_mode'
    REASON_START = 'reason_start'
    REASON_END = 'reason_end'
    SHUFFLE = 'shuffle'
    OFFLINE = 'offline'
    SKIPPED = 'skipped'

    TIMES_LISTENED = 'times_listened'
    SONG_MODE = 'mode'
    SONG_KEY = 'key'
    SONG_FULL_KEY = 'full_key'


@dataclass(frozen = True)
class Spdb:
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
    class TRACKS_LINKED_FROM:
        TBL_NAME = 'linked_tracks'

        FROM_ID = 'linked_from_id'
        RELINKED_ID = 'track_known_id'
        CREATED_AT = 'created_at'
        UPDATED_AT = 'updated_at'

    @dataclass(frozen = True)
    class TRACKS_LISTEN_HISTORY:
        TBL_NAME = 'tracks_listen_history'
        VIEW_NAME = 'v_tracks_listen_history'

        TIMESTAMP = 'time_stamp'
        USERNAME = 'username'
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

