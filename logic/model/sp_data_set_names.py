from dataclasses import dataclass
import config


@dataclass(frozen = True)
class PATH:
    JSON_FILE_PATH = config.JSON_FILE_PATH
    JSON_FILE_PREFIX = config.JSON_FILE_PREFIX


@dataclass(frozen = True)
class SPDT:
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

    # Analysis
    TIMES_LISTENED = 'times_listened'
    TOTAL_LISTEN_TIME = 'total_listen_time'
    MUSICAL_MODE = 'musical_mode'
    MUSICAL_KEY = 'musical_key'
    MUSICAL_FULL_KEY = 'musical_full_key'
