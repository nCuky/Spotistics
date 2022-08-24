from scipy.stats import trapezoid_gen
import datetime as dt
from SpotifyDataSet import SpotifyDataSet as spdt
# from pyspark.sql import SparkSession
import pandas as pd
# from app_gui import GUI as GUI
import logic as lg

if 0 == 1:
    features_data = lg.get_artist_audio_features_data(name="Frank Zappa")

else:
    track_data = spdt(aggr_level='track').get_data()
    track_data.sort_values(by='ts',
                           inplace=True,
                           ascending=True)

    track_data.to_csv(path_or_buf='data/personal_data/prepared/all_my_tracks_{0}.csv'.format(dt.datetime.now()),
                      encoding='utf-8')

# my_gui = GUI()
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


# user_playlists = spapic.get_all_user_playlists()
# test_plst = find_playlist(user_playlists, "Erez and Nadav")
# test_plst_tracks = spapic.get_all_playlist_tracks(test_plst)
# test_df = spapic.create_tracks_data_frame(tracks_items = test_plst_tracks,
#                                               audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])


# # ------------ local data csv's, local logic
# my_spoti_data = spdt()
# only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
# my_results = my_spoti_data.data.drop(columns='duration_ms').groupby('key').mean().assign(duration_ms=only_duration)
# my_results.to_csv("our_results.csv")


# my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")


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
