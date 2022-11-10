from logic.app_logic import Logic as lg
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib as mpl
import seaborn as sns
from logic.model.sp_data_set_names import SPDT as SPDT
from logic.frontend import plotting_names as PLTNM


class Defaults:
    font = {'family': 'sans-serif',
            'color': 'darkblue',
            'weight': 'normal',
            'size': 12}

    font_title = font.copy()
    font_title['weight'] = 'bold'
    font_title['size'] = 15


def top_artists_by_listen_count(logic: lg, top_artists_amount=50) -> None:
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
    plt_times_listens_by_artist = sns.barplot(x=SPDT.TIMES_LISTENED,
                                              y=times_listened_by_artist.index,
                                              data=times_listened_by_artist)

    plt.xlabel("Times listened", fontdict=Defaults.font)
    plt.ylabel("Artist", fontdict=Defaults.font)
    plt.title("My top 50 artists, by Number of listens to their tracks", fontdict=Defaults.font_title)
    plt.tight_layout(pad=1)
    plt.autoscale()
    plt_times_listens_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(500))
    plt_times_listens_by_artist.grid(b=True, axis='x')
    sns.set_style('darkgrid')

    plt.show()


def top_artists_by_total_listen_time(logic: lg, top_artists_amount=30) -> None:
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
    ax_artists_listen_time = sns.barplot(x=SPDT.TOTAL_LISTEN_TIME,
                                         y=total_listen_time_by_artist.index,
                                         data=total_listen_time_by_artist)

    plt.xlabel("Total time listened - in hours", fontdict=Defaults.font)
    plt.ylabel("Artist", fontdict=Defaults.font)
    plt.title(f"My top {top_artists_amount} artists, by "
              f"total listening time to their tracks", fontdict=Defaults.font_title)
    plt.tight_layout(pad=1)
    plt.autoscale()
    # plt_total_listen_time_by_artist.xaxis.set_major_locator(ticker.MultipleLocator(50))
    ax_artists_listen_time.grid(b=True, axis='x')
    ax_artists_listen_time.bar_label(ax_artists_listen_time.containers[0],
                                     fmt='%.1f%%',
                                     label_type='edge',
                                     padding=2)
    sns.set_style('darkgrid')

    plt.show()


def top_artists_albums_completion_percentage(logic: lg,
                                             top_artists_amount=10,
                                             min_track_listen_percentage=0.75) -> None:
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

    plt.figure('artists_albums_completion_percentage')
    plt_artist_albums_completion = sns.barplot(x=PLTNM.ARTIST_NAME,
                                               y=PLTNM.PERCENTAGE_LISTENED,
                                               data=artist_tracks_completion_df)

    plt.xlabel("Artist", fontdict=Defaults.font, labelpad=10)
    plt.ylabel("Listen percentage", fontdict=Defaults.font, labelpad=10)
    plt.title(f"My top {top_artists_amount} artists' listen completion percentage:\n"
              f"(How many tracks were listened, out of each artist's discography)", fontdict=Defaults.font_title)
    plt.tight_layout(pad=1, h_pad=1.4)
    plt.autoscale()
    # plt_artist_albums_completion.grid(b=True, axis='y')
    plt_artist_albums_completion.bar_label(plt_artist_albums_completion.containers[0],
                                           fmt='%.1f%%',
                                           label_type='edge',
                                           padding=2)

    # for index, row in artist_tracks_completion_df.iterrows():
    #     plt_artist_albums_completion.text(x = index,
    #                                       y = row[PLTNM.PERCENTAGE_LISTENED],
    #                                       s = f"{row[PLTNM.LISTENED_TRACKS].round()}/{row[PLTNM.TOTAL_TRACKS].round()} tracks",
    #                                       ha = 'center')
    #
    # for c in plt_artist_albums_completion.containers[0].get_children():
    #     plt_artist_albums_completion.text(x=c.xy[0], y=c.xy[1],
    #                                       s=f"{artist_tracks_completion_df.iloc[c.xy[0]][PLTNM.LISTENED_TRACKS]}/{artist_tracks_completion_df.iloc[c.xy[0]][PLTNM.TOTAL_TRACKS]}",
    #                                       ha='center')
    # for i, c in enumerate(plt_artist_albums_completion.containers[0].datavalues):
    #     plt_artist_albums_completion.text(x=plt_artist_albums_completion.containers[0][i].xy[0],
    #                                       y=plt_artist_albums_completion.containers[0][i].xy[1],
    #                                       s=f"{artist_tracks_completion_df.iloc[plt_artist_albums_completion.containers[0][i].xy[0]][PLTNM.LISTENED_TRACKS]}/{artist_tracks_completion_df.iloc[plt_artist_albums_completion.containers[0][i].xy[0]][PLTNM.TOTAL_TRACKS]}",
    #                                       ha='center')

    # .patches is everything inside the chart
    for i, rect in enumerate(plt_artist_albums_completion.patches):
        # Find where everything is located
        height = rect.get_height()
        width = rect.get_width()
        x = rect.get_x()
        y = rect.get_y()

        artist_srs = artist_tracks_completion_df.iloc[i]

        # The height of the bar is the data value and can be used as the label
        label_text = f"{artist_srs[PLTNM.LISTENED_TRACKS]}/{artist_srs[PLTNM.TOTAL_TRACKS]} tracks"

        # ax.text(x, y, text)
        label_x = x + width / 2
        label_y = y + height

        # plot only when height is greater than specified value
        if height > 0:
            plt_artist_albums_completion.text(label_x, label_y, label_text, ha='center', va='top', fontsize=8)

    sns.set_style('darkgrid')

    plt.show()
