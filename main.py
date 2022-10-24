from logic import Logic as lg
import plotting as plt

# Initializing the application:
my_lg = lg(listen_history_from = 'db')

plt.top_artists_by_listen_count(my_lg)
plt.top_artists_by_total_listen_time(my_lg)
plt.top_artists_albums_completion_percentage(my_lg)

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

