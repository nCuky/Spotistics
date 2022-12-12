from logic.app_logic import Logic as lg
from logic.frontend import plotting as plt
import config

# Initializing the application:
my_lg = lg(listen_history_from = config.LISTEN_HISTORY_SRC)

plt.top_artists_by_listen_count(my_lg)
plt.top_artists_by_total_listen_time(my_lg)
plt.top_artists_albums_completion_percentage(my_lg)
plt.top_tracks_audio_features(my_lg)
