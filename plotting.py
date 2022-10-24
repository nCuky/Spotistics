import pandas as pd
import numpy as np
from db_names import V_KNOWN_LISTEN_HISTORY as DF_NAMES
from logic import Logic as lg
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from sp_data_set_names import SPDT as SPDT


def top_artists_by_listen_count(logic: lg) -> None:
    tracks_count = logic.agg_unique_tracks_by_listens().sort_values(by = SPDT.TOTAL_LISTEN_TIME, ascending = False)

    times_listened_by_artist = tracks_count.groupby(by = SPDT.ALBUM_ARTIST_NAME, as_index = True).agg(
        times_listened = (SPDT.TIMES_LISTENED, 'sum')).sort_values(SPDT.TIMES_LISTENED,
                                                                   ascending = False).head(50)

    plt_times_listens_by_artist = sns.barplot(x = SPDT.TIMES_LISTENED,
                                              y = times_listened_by_artist.index,
                                              data = times_listened_by_artist)
    plt_times_listens_by_artist.set(xlabel = "Times listened",
                                    ylabel = "Artist",
                                    title = "My top 50 artists, by Number of listens to their tracks")
    plt_times_listens_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(500))
    plt_times_listens_by_artist.grid(b = True, axis = 'x')

    sns.set_style('darkgrid')

    plt.tight_layout()
    plt.show()


def top_artists_by_total_listen_time(logic: lg, top_artists_amount = 30) -> None:
    tracks_count = logic.agg_unique_tracks_by_listens().sort_values(by = SPDT.TOTAL_LISTEN_TIME, ascending = False)

    total_listen_time_by_artist = tracks_count.groupby(by = SPDT.ALBUM_ARTIST_NAME, as_index = True).agg(
        total_listen_time = (SPDT.TOTAL_LISTEN_TIME, 'sum')).sort_values(SPDT.TOTAL_LISTEN_TIME,
                                                                         ascending = False).head(top_artists_amount)

    total_listen_time_by_artist[SPDT.TOTAL_LISTEN_TIME] = total_listen_time_by_artist[SPDT.TOTAL_LISTEN_TIME].apply(
        lambda val: val / 1000 / 60 / 60 / 24)

    plt_total_listen_time_by_artist = sns.barplot(x = SPDT.TOTAL_LISTEN_TIME,
                                                  y = total_listen_time_by_artist.index,
                                                  data = total_listen_time_by_artist)

    plt_total_listen_time_by_artist.set(xlabel = "Total time listened - in days",
                                        ylabel = "Artist",
                                        title = f"My top {top_artists_amount} artists, by "
                                                f"total listening time to their tracks")
    # plt_total_listen_time_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(50))
    plt_total_listen_time_by_artist.grid(b = True, axis = 'x')

    sns.set_style('darkgrid')

    plt.tight_layout()
    plt.show()


def top_artists_albums_completion_percentage(logic: lg,
                                             top_artists_amount = 10,
                                             min_track_listen_percentage = 0.75) -> None:
    """
    Plots a graph showing how much of an artist's discography was listened to, for each top artist
    (top artists are determined by total listen time to their tracks regardless of discography).

    Parameters:
        logic: Main app's logic object.

        top_artists_amount: Amount of artists considered "Top Artists" for displaying.

        min_track_listen_percentage: Decimal value between 0 and 1 (including boundaries), determining
            the percentage of a track's duration that should have been played in order for the track to be
            considered as "listened" for the calculation.

            Examples: 0.30 = at least 30% of the track should have been played.
            1.00 = the whole track should have been played.
            0.00 = even if the track was only skipped through, without being played at all, but still being documented
            in the listen history, it is considered as 'listened'.

    Returns:
        None.
    """
    if min_track_listen_percentage < 0:
        min_track_listen_percentage = 0

    elif min_track_listen_percentage > 1:
        min_track_listen_percentage = 1

    tracks_count = logic.agg_unique_tracks_by_listens().sort_values(by = SPDT.TOTAL_LISTEN_TIME, ascending = False)

    total_listen_time_by_artist = tracks_count.groupby(by = DF_NAMES.ALBUM_ARTIST_ID,
                                                       as_index = True).agg(
        total_listen_time = (SPDT.TOTAL_LISTEN_TIME, 'sum'),
        album_artist_name = (DF_NAMES.ALBUM_ARTIST_NAME, 'first')).sort_values(
        by = SPDT.TOTAL_LISTEN_TIME, ascending = False).head(top_artists_amount)

    artists_ids = total_listen_time_by_artist.index.to_list()

    listen_history_df = logic.get_listen_history_df()
    listen_history_df = listen_history_df.drop(index = listen_history_df.index[
        ~listen_history_df[DF_NAMES.ALBUM_ARTIST_ID].isin(artists_ids)], inplace = False)

    history_max_played = listen_history_df.sort_values(by = [DF_NAMES.TRACK_KNOWN_ID, DF_NAMES.MS_PLAYED],
                                                       ascending = True).drop_duplicates(DF_NAMES.TRACK_KNOWN_ID,
                                                                                         keep = 'last')
    history_max_played.set_index(DF_NAMES.TRACK_KNOWN_ID, inplace = True)

    history_max_played['is_considered_listened'] = np.select(
        [history_max_played[DF_NAMES.MS_PLAYED] >= history_max_played[
            DF_NAMES.TRACK_DURATION_MS] * min_track_listen_percentage], [True], False)

    artists_tracks = logic.spapi.artists_get_all_tracks(artists_ids)

    artists_tracks_flat = {}

    for artist_id, albums in artists_tracks.items():
        for album_tuple in albums:
            for simple_track in album_tuple[1]:
                try:
                    track_listened = history_max_played.loc[simple_track.id]['is_considered_listened']

                except KeyError:
                    track_listened = False

                if artists_tracks_flat.get(artist_id) is None:
                    artists_tracks_flat[artist_id] = {}

                artists_tracks_flat[artist_id][simple_track.id] = track_listened

    series_list = []
    series_cols = ['artist_id', 'track_id', 'is_track_listened']

    for artist_id, tracks in artists_tracks_flat.items():
        for track_id, is_track_listened in tracks.items():
            artist_tracks_completion_sr = pd.Series([artist_id, track_id, is_track_listened], index = series_cols)
            series_list.append(artist_tracks_completion_sr)

    artist_tracks_completion_df = pd.DataFrame(series_list, columns = series_cols)

    artist_tracks_completion_df = artist_tracks_completion_df.groupby('artist_id').agg(
        artist_id = ('artist_id', 'first'),
        listened_tracks = ('is_track_listened', 'sum'),
        total_tracks = ('track_id', 'count')).set_index('artist_id')
    artist_tracks_completion_df.sort_index(ascending = True, inplace = True)

    names = listen_history_df[listen_history_df[DF_NAMES.ALBUM_ARTIST_ID].isin(artist_tracks_completion_df.index)][
        [DF_NAMES.ALBUM_ARTIST_ID, DF_NAMES.ALBUM_ARTIST_NAME]].drop_duplicates().set_index(DF_NAMES.ALBUM_ARTIST_ID)
    names.sort_index(ascending = True, inplace = True)

    artist_tracks_completion_df['artist_name'] = names

    plt_artist_albums_completion = sns.barplot(x = artist_tracks_completion_df['artist_name'],
                                               y = artist_tracks_completion_df['listened_tracks'].divide(
                                                   artist_tracks_completion_df['total_tracks']) * 100,
                                               data = artist_tracks_completion_df)

    plt_artist_albums_completion.set(xlabel = "Artist",
                                     ylabel = "Listen percentage",
                                     title = f"My top {top_artists_amount} artists' listen completion percentage:\n"
                                             f"(How many tracks were listened, out of each artist's discography)")

    plt_artist_albums_completion.grid(b = True, axis = 'y')

    sns.set_style('darkgrid')

    plt.tight_layout()
    plt.show()
