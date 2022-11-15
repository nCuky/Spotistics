from logic.app_logic import Logic as lg
from logic.frontend import plotting as plt

# Initializing the application:
my_lg = lg(listen_history_from = 'json')

# plt.top_artists_by_listen_count(my_lg)
# plt.top_artists_by_total_listen_time(my_lg)
plt.top_artists_albums_completion_percentage(my_lg)
