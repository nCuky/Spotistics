from logic import Logic as lg

# Plotting
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from sp_data_set_names import Spdt as spdt

# Initializing the application:
my_lg = lg(listen_history_from = 'db')

# region Plotting

tracks_count = my_lg.count_unique_tracks().sort_values(by = spdt.TOTAL_LISTEN_TIME, ascending = False)

# # region Times Listened by Artist
#
# times_listened_by_artist = tracks_count.groupby(by = spdt.ALBUM_ARTIST_NAME, as_index = True).agg(
#     times_listened = (spdt.TIMES_LISTENED, 'sum')).sort_values(spdt.TIMES_LISTENED,
#                                                                ascending = False).head(50)
#
# plt_times_listens_by_artist = sns.barplot(x = spdt.TIMES_LISTENED,
#                                           y = times_listened_by_artist.index,
#                                           data = times_listened_by_artist)
# plt_times_listens_by_artist.set(xlabel = "Times listened",
#                                 ylabel = "Artist",
#                                 title = "My top 50 artists, by Number of listens to their tracks")
# plt_times_listens_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(500))
# plt_times_listens_by_artist.grid(b = True, axis = 'x')
#
# sns.set_style('darkgrid')
#
# plt.tight_layout()
# plt.show()

# # endregion Times Listened by Artist


# region Total Time Listened by Artists

total_listen_time_by_artist = tracks_count.groupby(by = spdt.ALBUM_ARTIST_NAME, as_index = True).agg(
    total_listen_time = (spdt.TOTAL_LISTEN_TIME, 'sum')).sort_values(spdt.TOTAL_LISTEN_TIME,
                                                                     ascending = False).head(30)

total_listen_time_by_artist[spdt.TOTAL_LISTEN_TIME] = total_listen_time_by_artist[spdt.TOTAL_LISTEN_TIME].apply(
    lambda val: val / 1000 / 60 / 60)

plt_total_listen_time_by_artist = sns.barplot(x = spdt.TOTAL_LISTEN_TIME,
                                              y = total_listen_time_by_artist.index,
                                              data = total_listen_time_by_artist)

plt_total_listen_time_by_artist.set(xlabel = "Total time listened - in hours",
                                    ylabel = "Artist",
                                    title = "My top 30 artists, by total listening time to their tracks")
plt_total_listen_time_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(50))
plt_total_listen_time_by_artist.grid(b = True, axis = 'x')

# endregion Total Time Listened by Artists

sns.set_style('darkgrid')

plt.tight_layout()
plt.show()

# endregion Plotting


# my_gui = AppGUI()
#
# my_gui.close()


x = 1  # break

# # PySpark
# print(str(datetime.datetime.now()) + ' --- Building Spark Session...')
# spark = SparkSession.builder.appName('Practice').getOrCreate()
#
# print(str(datetime.datetime.now()) + ' --- Reading CSV... ')
# df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema = True)
#
# print(str(datetime.datetime.now()) + ' --- Doing stuff... ')
# df_pyspark.withColumnRenamed('track_name','name').show()


# user_playlists = my_spapi.get_all_user_playlists()
# test_plst = find_playlist(user_playlists, "Erez and Nadav")
# test_plst_tracks = my_spapi.get_all_playlist_tracks(test_plst)
# test_df = my_spapi.create_tracks_data_frame(tracks_items = test_plst_tracks,
#                                               audio_features_names = ['instrumentalness', 'energy', 'danceability',
#                                                                       'acousticness', 'tempo'])


#
# def calc_listen_data_by_key():
#     '''
#     Aggregates all listened tracks by key, and writes it as a csv file
#     :return:
#     '''
#     my_spoti_data = spdt(aggr_level='track')
#     only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
#     my_results = my_spoti_data.data.drop(columns='duration_ms').groupby('key').mean().assign(
#         duration_ms=only_duration)
#     my_results.to_csv("listen_data_by_key.csv")
#
#
# def calc_listen_data_mean_key():
#     '''
#     Aggregates all listened tracks by mean key
#     :return:
#     '''
#     my_spoti_data = spdt(aggr_level='track')
#     my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")
