from logic.app_logic import Logic as Lg
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from logic.model.sp_data_set_names import SPDT as SPDT
from logic.frontend import plotting_names as PLTNM


class Defaults:
    font = {'family': 'sans-serif',
            'color' : 'darkblue',
            'weight': 'normal',
            'size'  : 12}

    font_title = font.copy()
    font_title['weight'] = 'bold'
    font_title['size'] = 15


def top_artists_by_listen_count(logic: Lg, top_artists_amount = 50) -> None:
    """
    Plots a graph showing the Top Artists, determined by the total number of times each artist's tracks were listened.

    Parameters:
        logic: Main app's logic object.

        top_artists_amount: Amount of artists considered "Top Artists" for displaying.

    Returns:
        None.
    """
    times_listened_by_artist = logic.calc_top_artists_by_listen_count(top_artists_amount)

    plt.figure('top_artists_by_listen_count')
    plt_times_listens_by_artist = sns.barplot(x = SPDT.TIMES_LISTENED,
                                              y = times_listened_by_artist.index,
                                              data = times_listened_by_artist)

    plt.xlabel("Times listened", fontdict = Defaults.font)
    plt.ylabel("Artist", fontdict = Defaults.font)
    plt.title("My top 50 artists, by Number of listens to their tracks", fontdict = Defaults.font_title)
    plt.tight_layout(pad = 1)
    plt.autoscale()
    plt_times_listens_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(500))
    plt_times_listens_by_artist.grid(b = True, axis = 'x')
    sns.set_style('darkgrid')

    plt.show()


def top_artists_by_total_listen_time(logic: Lg, top_artists_amount = 30) -> None:
    """
    Plots a graph showing the Top Artists, determined by the total listen time of each artist's tracks.

    Parameters:
        logic: Main app's logic object.

        top_artists_amount: Amount of artists considered "Top Artists" for displaying.

    Returns:
        None.
    """
    total_listen_time_by_artist = logic.calc_top_artists_by_total_listen_time(top_artists_amount)

    plt.figure('top_artists_by_total_listen_time')
    ax_artists_listen_time = sns.barplot(x = SPDT.TOTAL_LISTEN_TIME,
                                         y = total_listen_time_by_artist.index,
                                         data = total_listen_time_by_artist)

    plt.xlabel("Total time listened - in hours", fontdict = Defaults.font)
    plt.ylabel("Artist", fontdict = Defaults.font)
    plt.title(f"My top {top_artists_amount} artists, by "
              f"total listening time to their tracks", fontdict = Defaults.font_title)
    plt.tight_layout(pad = 1)
    plt.autoscale()
    # plt_total_listen_time_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(50))
    ax_artists_listen_time.grid(b = True, axis = 'x')
    ax_artists_listen_time.bar_label(ax_artists_listen_time.containers[0],
                                     fmt = '%.1f%%',
                                     label_type = 'edge',
                                     padding = 2)
    sns.set_style('darkgrid')

    plt.show()


def top_artists_albums_completion_percentage(logic: Lg,
                                             top_artists_amount = 10,
                                             min_track_listen_percentage = 0.75) -> None:
    """
    Plots a graph showing how much of an artist's discography was listened to, for each top artist
    (top artists are determined by total listen time to their tracks regardless of discography).

    Only "regular" albums and "appears on" albums are counted as an artist's "discography",
    without singles or compilation albums.

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
    artist_tracks_completion_df = logic.calc_top_artists_albums_completion(top_artists_amount,
                                                                           min_track_listen_percentage)

    plt.figure('Artist completion percentage', figsize = (15, 7))
    plt_artist_albums_completion = sns.barplot(x = PLTNM.ARTIST_NAME,
                                               y = PLTNM.PERCENTAGE_LISTENED,
                                               data = artist_tracks_completion_df)

    plt.yticks(range(0, 101, 20))
    plt_artist_albums_completion.tick_params(axis = 'x', pad = 0.5, labelrotation = 10, labelsize = 11)
    plt.xlabel("Artist", fontdict = Defaults.font, labelpad = 10)
    plt.ylabel("Listen percentage", fontdict = Defaults.font, labelpad = 10)
    plt.title(f"My top {top_artists_amount} artists' listen completion percentage:\n"
              f"(How many tracks were listened, out of each artist's discography)", fontdict = Defaults.font_title)
    plt.autoscale(enable = True, axis = 'x')
    plt.tight_layout(pad = 1, h_pad = 1.2)

    for i, rect in enumerate(plt_artist_albums_completion.patches):
        # Find where everything is located
        height = rect.get_height()
        width = rect.get_width()
        x = rect.get_x()
        y = rect.get_y()

        artist_srs = artist_tracks_completion_df.iloc[i]

        # The height of the bar is the data value and can be used as the label
        label_text = f"{round(artist_srs[PLTNM.PERCENTAGE_LISTENED], ndigits = 1)}%\n" \
                     + f"{artist_srs[PLTNM.LISTENED_TRACKS]}/{artist_srs[PLTNM.TOTAL_TRACKS]} tracks"

        label_x = x + width / 2
        label_y = y + height

        # plot only when height is greater than specified value
        if height > 0:
            plt_artist_albums_completion.text(x = label_x, y = label_y, s = label_text,
                                              ha = 'center', va = 'bottom', fontsize = 10)

    sns.set_style('darkgrid')
    plt.show()


def top_tracks_audio_features(logic: Lg,
                              top_tracks_amount = 30) -> None:
    """
    Plots a graph showing the AudioFeatures that are common among the top tracks
    (top tracks are determined by total listen time to each track).

    Parameters:
        logic: Main app's logic object.

        top_tracks_amount: Amount of tracks to be considered "Top Tracks" for displaying.

    Returns:
        None.
    """
    top_tracks_features = logic.calc_audio_features_for_top_tracks(top_tracks_amount)

