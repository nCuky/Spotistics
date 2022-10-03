import names
import spotify_data_set as spdt
# from pyspark.sql import SparkSession
# from app_gui import AppGUI as AppGUI
from logic import Logic as lg

# Plotting
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from names import Spdb as spdb
from names import Spdt as spdt

my_lg = lg()

# features_data = my_lg.get_artist_audio_features_data(name = "Frank Zappa")
# my_lg.collect_all_tracks_to_file()

my_lg.collect_data_and_save()
tracks_count = my_lg.count_unique_tracks()
tracks_count.sort_values(by = spdt.TIMES_LISTENED, ascending = False, inplace = True)

# region Plotting


total_listens_by_artist = tracks_count.groupby(by = spdt.ALBUM_ARTIST_NAME, as_index = True).agg(
    total_listened = (spdt.TIMES_LISTENED, 'sum')).sort_values('total_listened', ascending = False).head(50)

sns.set_style('darkgrid')

plt_listens_by_artist = sns.barplot(x = 'total_listened',
                                    y = total_listens_by_artist.index,
                                    data = total_listens_by_artist)

plt_listens_by_artist.set(xlabel = "Total times listened",
                          ylabel = "Artist",
                          title = "My top 50 artists by number of listens")

plt_listens_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(500))

plt.tight_layout()

# plt_listens_by_artist.set_xticklabels(plt_listens_by_artist.get_xticklabels(), rotation = 45,
#                                       horizontalalignment = 'right')
plt_listens_by_artist.grid(b = True, axis = 'x')

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
#                                               audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])


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
